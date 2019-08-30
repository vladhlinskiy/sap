/*
 * Copyright © 2019 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package io.cdap.plugin.sap;

import com.google.common.base.Preconditions;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.batch.Input;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.batch.BatchRuntimeContext;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.api.batch.BatchSourceContext;
import io.cdap.cdap.etl.api.validation.InvalidConfigPropertyException;
import io.cdap.cdap.etl.api.validation.InvalidStageException;
import io.cdap.plugin.common.LineageRecorder;
import io.cdap.plugin.sap.exception.ODataException;
import org.apache.hadoop.io.NullWritable;
import org.apache.olingo.odata2.api.edm.EdmAnnotationAttribute;
import org.apache.olingo.odata2.api.edm.EdmAnnotations;
import org.apache.olingo.odata2.api.edm.EdmEntityType;
import org.apache.olingo.odata2.api.edm.EdmException;
import org.apache.olingo.odata2.api.edm.EdmProperty;
import org.apache.olingo.odata2.api.edm.EdmTyped;
import org.apache.olingo.odata2.api.ep.entry.ODataEntry;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Plugin returns records from SAP OData service specified by URL.
 */
@Plugin(type = BatchSource.PLUGIN_TYPE)
@Name(SapODataConstants.PLUGIN_NAME)
@Description("Read data from SAP OData service.")
public class SapODataSource extends BatchSource<NullWritable, ODataEntry, StructuredRecord> {

  private final SapODataConfig config;
  private ODataEntryToRecordTransformer transformer;

  public SapODataSource(SapODataConfig config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    config.validate();
    try {
      // API call validation
      new OData2Client(config.getUrl(), config.getUser(), config.getPassword())
        .getEntitySetType(config.getResourcePath());
    } catch (ODataException e) {
      throw new InvalidConfigPropertyException("Unable to connect to OData Service: " + e.getMessage(), e,
                                               SapODataConstants.ODATA_SERVICE_URL);
    }

    Schema schema = getSchema();
    Schema configuredSchema = config.getParsedSchema();
    if (configuredSchema == null) {
      pipelineConfigurer.getStageConfigurer().setOutputSchema(schema);
      return;
    }

    try {
      SapODataConfig.validateFieldsMatch(schema, configuredSchema);
      pipelineConfigurer.getStageConfigurer().setOutputSchema(configuredSchema);
    } catch (IllegalArgumentException e) {
      throw new InvalidConfigPropertyException(e.getMessage(), e, "schema");
    }
  }

  @Override
  public void prepareRun(BatchSourceContext context) {
    config.validate();
    try {
      // API call validation
      new OData2Client(config.getUrl(), config.getUser(), config.getPassword())
        .getEntitySetType(config.getResourcePath());
    } catch (ODataException e) {
      throw new InvalidConfigPropertyException("Unable to connect to OData Service: " + e.getMessage(), e,
                                               SapODataConstants.ODATA_SERVICE_URL);
    }

    Schema schema = context.getOutputSchema();
    LineageRecorder lineageRecorder = new LineageRecorder(context, config.getReferenceName());
    lineageRecorder.createExternalDataset(schema);
    lineageRecorder.recordRead("Read", String.format("Read resource '%s' from OData service '%s'",
                                                     config.getResourcePath(), config.getUrl()),
                               Preconditions.checkNotNull(schema.getFields()).stream()
                                 .map(Schema.Field::getName)
                                 .collect(Collectors.toList()));

    context.setInput(Input.of(config.getReferenceName(), new ODataEntryInputFormatProvider(config)));
  }

  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    super.initialize(context);
    Schema schema = context.getOutputSchema();
    this.transformer = config.isIncludeMetadataAnnotations()
      ? new ODataEntryToRecordWithMetadataTransformer(schema, getMetadataAnnotations())
      : new ODataEntryToRecordTransformer(schema);
  }

  @Override
  public void transform(KeyValue<NullWritable, ODataEntry> input, Emitter<StructuredRecord> emitter) {
    ODataEntry entity = input.getValue();
    emitter.emit(transformer.transform(entity));
  }

  public Schema getSchema() {
    OData2Client oData2Client = new OData2Client(config.getUrl(), config.getUser(), config.getPassword());
    try {
      EdmEntityType edmEntityType = oData2Client.getEntitySetType(config.getResourcePath());
      List<String> selectProperties = config.getSelectProperties();
      List<Schema.Field> fields = new ArrayList<>();
      for (String propertyName : edmEntityType.getPropertyNames()) {
        if (!selectProperties.isEmpty() && !selectProperties.contains(propertyName)) {
          continue;
        }
        EdmTyped property = edmEntityType.getProperty(propertyName);
        fields.add(getSchemaField(property, config.isIncludeMetadataAnnotations()));
      }
      return Schema.recordOf("output", fields);
    } catch (EdmException | ODataException e) {
      throw new InvalidStageException("Unable to get details about the entity type: " + e.getMessage(), e);
    }
  }

  // TODO duplication
  private Map<String, Map<String, String>> getMetadataAnnotations() {

    OData2Client oData2Client = new OData2Client(config.getUrl(), config.getUser(), config.getPassword());
    try {
      EdmEntityType edmEntityType = oData2Client.getEntitySetType(config.getResourcePath());
      List<String> selectProperties = config.getSelectProperties();
      Map<String, Map<String, String>> fieldNameToMetadata = new HashMap<>();
      for (String propertyName : edmEntityType.getPropertyNames()) {
        if (!selectProperties.isEmpty() && !selectProperties.contains(propertyName)) {
          continue;
        }

        EdmProperty property = (EdmProperty) edmEntityType.getProperty(propertyName);
        List<EdmAnnotationAttribute> annotationAttributes = property.getAnnotations().getAnnotationAttributes();
        if (annotationAttributes == null) {
          continue;
        }

        Map<String, String> fieldMetadata = annotationAttributes.stream()
          .collect(Collectors.toMap(EdmAnnotationAttribute::getName, EdmAnnotationAttribute::getText));
        fieldNameToMetadata.put(property.getName(), fieldMetadata);
      }

      return fieldNameToMetadata;
    } catch (EdmException | ODataException e) {
      throw new InvalidStageException("Unable to get metadata annotations for the entity type: " + e.getMessage(), e);
    }
  }

  private Schema.Field getSchemaField(EdmTyped edmTyped, boolean includeAnnotations) throws EdmException {
    EdmProperty property = (EdmProperty) edmTyped;
    Schema nonNullableSchema = convertPropertyType(property);
    Schema schema = property.getFacets().isNullable() ? Schema.nullableOf(nonNullableSchema) : nonNullableSchema;

    boolean annotationsExist = property.getAnnotations().getAnnotationAttributes() != null;
    return includeAnnotations && annotationsExist ?
      getFieldWithAnnotations(property, schema) : Schema.Field.of(property.getName(), schema);
  }

  private Schema.Field getFieldWithAnnotations(EdmProperty property, Schema valueSchema) throws EdmException {
    EdmAnnotations annotations = property.getAnnotations();
    List<Schema.Field> fields = annotations.getAnnotationAttributes().stream()
      .map(a -> Schema.Field.of(a.getName(), Schema.of(Schema.Type.STRING)))
      .collect(Collectors.toList());

    Schema.Field metadataFiled = Schema.Field.of(SapODataConstants.METADATA_ANNOTATIONS_FIELD_NAME,
                                                 Schema.recordOf(property.getName() + "-metadata-record", fields));

    Schema valueWithMetadataSchema = Schema.recordOf(property.getName() + "-value-with-metadata-record",
                                                     Schema.Field.of(SapODataConstants.VALUE_FIELD_NAME, valueSchema),
                                                     metadataFiled);
    return Schema.Field.of(property.getName(), valueWithMetadataSchema);
  }

  private Schema convertPropertyType(EdmProperty edmProperty) throws EdmException {
    switch (edmProperty.getType().getName()) {
      case "Binary":
        return Schema.of(Schema.Type.BYTES);
      case "Boolean":
        return Schema.of(Schema.Type.BOOLEAN);
      case "Byte":
        return Schema.of(Schema.Type.INT);
      case "SByte":
        return Schema.of(Schema.Type.INT);
      case "DateTime":
        return Schema.of(Schema.LogicalType.TIMESTAMP_MICROS);
      case "DateTimeOffset":
        // Mapped to 'string' to avoid timezone information loss
        return Schema.of(Schema.Type.STRING);
      case "Time":
        return Schema.of(Schema.LogicalType.TIME_MICROS);
      case "Decimal":
        return Schema.decimalOf(edmProperty.getFacets().getPrecision(), edmProperty.getFacets().getScale());
      case "Double":
        return Schema.of(Schema.Type.DOUBLE);
      case "Single":
        return Schema.of(Schema.Type.FLOAT);
      case "Guid":
        return Schema.of(Schema.Type.STRING);
      case "Int16":
        return Schema.of(Schema.Type.INT);
      case "Int32":
        return Schema.of(Schema.Type.INT);
      case "Int64":
        return Schema.of(Schema.Type.LONG);
      case "String":
        return Schema.of(Schema.Type.STRING);
      default:
        // this should never happen
        throw new InvalidStageException(String.format("Field '%s' is of unsupported type '%s'.", edmProperty.getName(),
                                                      edmProperty.getType().getName()));
    }
  }
}
