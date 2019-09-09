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
import io.cdap.plugin.sap.odata.EntityType;
import io.cdap.plugin.sap.odata.GenericODataClient;
import io.cdap.plugin.sap.odata.ODataEntity;
import io.cdap.plugin.sap.odata.Property;
import io.cdap.plugin.sap.odata.exception.ODataException;
import io.cdap.plugin.sap.transformer.ODataEntryToRecordTransformer;
import io.cdap.plugin.sap.transformer.ODataEntryToRecordWithMetadataTransformer;
import org.apache.hadoop.io.NullWritable;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Plugin returns records from SAP OData service specified by URL.
 */
@Plugin(type = BatchSource.PLUGIN_TYPE)
@Name(SapODataConstants.PLUGIN_NAME)
@Description("Read data from SAP OData service.")
public class SapODataSource extends BatchSource<NullWritable, ODataEntity, StructuredRecord> {

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
      new GenericODataClient(config.getUrl(), config.getUser(), config.getPassword())
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
      new GenericODataClient(config.getUrl(), config.getUser(), config.getPassword())
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
  public void transform(KeyValue<NullWritable, ODataEntity> input, Emitter<StructuredRecord> emitter) {
    ODataEntity entity = input.getValue();
    emitter.emit(transformer.transform(entity));
  }

  public Schema getSchema() {
    GenericODataClient oDataClient = new GenericODataClient(config.getUrl(), config.getUser(), config.getPassword());
    try {
      EntityType entityType = oDataClient.getEntitySetType(config.getResourcePath());
      List<Schema.Field> fields = entityType.getProperties().stream()
        .filter(p -> config.getSelectProperties().isEmpty() || config.getSelectProperties().contains(p.getName()))
        .map(property -> getSchemaField(property, config.isIncludeMetadataAnnotations()))
        .collect(Collectors.toList());
      return Schema.recordOf("output", fields);
    } catch (ODataException e) {
      throw new InvalidStageException("Unable to get details about the entity type: " + e.getMessage(), e);
    }
  }

  // TODO currently works only for OData 2
  private Map<String, Map<String, String>> getMetadataAnnotations() {
    GenericODataClient oDataClient = new GenericODataClient(config.getUrl(), config.getUser(), config.getPassword());
    try {
      EntityType entityType = oDataClient.getEntitySetType(config.getResourcePath());
      return entityType.getProperties().stream()
        .filter(p -> config.getSelectProperties().isEmpty() || config.getSelectProperties().contains(p.getName()))
        .collect(Collectors.toMap(Property::getName, Property::getAnnotations));
    } catch (ODataException e) {
      throw new InvalidStageException("Unable to get metadata annotations for the entity type: " + e.getMessage(), e);
    }
  }

  private Schema.Field getSchemaField(Property property, boolean includeAnnotations) {
    Schema nonNullableSchema = convertPropertyType(property);
    Schema schema = property.isNullable() ? Schema.nullableOf(nonNullableSchema) : nonNullableSchema;

    return includeAnnotations && property.getAnnotations() != null ?
      getFieldWithAnnotations(property, schema) : Schema.Field.of(property.getName(), schema);
  }

  private Schema.Field getFieldWithAnnotations(Property property, Schema valueSchema) {
    Map<String, String> annotations = property.getAnnotations();
    List<Schema.Field> fields = annotations.keySet().stream()
      .map(name -> Schema.Field.of(name, Schema.of(Schema.Type.STRING)))
      .collect(Collectors.toList());

    Schema.Field metadataFiled = Schema.Field.of(SapODataConstants.METADATA_ANNOTATIONS_FIELD_NAME,
                                                 Schema.recordOf(property.getName() + "-metadata-record", fields));

    Schema valueWithMetadataSchema = Schema.recordOf(property.getName() + "-value-with-metadata-record",
                                                     Schema.Field.of(SapODataConstants.VALUE_FIELD_NAME, valueSchema),
                                                     metadataFiled);
    return Schema.Field.of(property.getName(), valueWithMetadataSchema);
  }

  // TODO add OData 4 data types
  private Schema convertPropertyType(Property property) {
    switch (property.getEdmTypeName()) {
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
        return Schema.decimalOf(property.getPrecision(), property.getScale());
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
      // TODO
      case "GeographyPoint":
        return Schema.of(Schema.Type.STRING);
      case "GeographyLineString":
        return Schema.of(Schema.Type.STRING);
      case "GeographyPolygon":
        return Schema.of(Schema.Type.STRING);
      case "GeographyMultiPoint":
        return Schema.of(Schema.Type.STRING);
      case "GeographyMultiLineString":
        return Schema.of(Schema.Type.STRING);
      case "GeographyMultiPolygon":
        return Schema.of(Schema.Type.STRING);
      case "GeographyCollection":
        return Schema.of(Schema.Type.STRING);
      case "GeometryPoint":
        return Schema.of(Schema.Type.STRING);
      case "GeometryLineString":
        return Schema.of(Schema.Type.STRING);
      case "GeometryPolygon":
        return Schema.of(Schema.Type.STRING);
      case "GeometryMultiPoint":
        return Schema.of(Schema.Type.STRING);
      case "GeometryMultiLineString":
        return Schema.of(Schema.Type.STRING);
      case "GeometryMultiPolygon":
        return Schema.of(Schema.Type.STRING);
      case "GeometryCollection":
        return Schema.of(Schema.Type.STRING);
      case "Date":
        return Schema.of(Schema.Type.STRING); // TODO check string representation
      case "Duration":
        return Schema.of(Schema.Type.STRING); // TODO check string representation
      case "Stream":
        return Schema.of(Schema.Type.STRING); // TODO ignore it
      case "TimeOfDay":
        return Schema.of(Schema.Type.STRING); // TODO check string representation
      default:
        // this should never happen
        throw new InvalidStageException(String.format("Field '%s' is of unsupported type '%s'.", property.getName(),
                                                      property.getEdmTypeName()));
    }
  }
}
