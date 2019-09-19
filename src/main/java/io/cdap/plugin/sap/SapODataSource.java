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
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.StageConfigurer;
import io.cdap.cdap.etl.api.batch.BatchRuntimeContext;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.api.batch.BatchSourceContext;
import io.cdap.cdap.etl.api.validation.InvalidStageException;
import io.cdap.plugin.common.LineageRecorder;
import io.cdap.plugin.sap.odata.EntityType;
import io.cdap.plugin.sap.odata.GenericODataClient;
import io.cdap.plugin.sap.odata.ODataAnnotation;
import io.cdap.plugin.sap.odata.ODataEntity;
import io.cdap.plugin.sap.odata.PropertyMetadata;
import io.cdap.plugin.sap.odata.exception.ODataException;
import io.cdap.plugin.sap.odata.odata2.OData2Annotation;
import io.cdap.plugin.sap.odata.odata4.OData4Annotation;
import io.cdap.plugin.sap.transformer.ODataEntryToRecordTransformer;
import io.cdap.plugin.sap.transformer.ODataEntryToRecordWithMetadataTransformer;
import org.apache.hadoop.io.NullWritable;
import org.apache.olingo.commons.api.edm.provider.annotation.CsdlDynamicExpression;
import org.apache.olingo.commons.api.edm.provider.annotation.CsdlExpression;
import org.apache.olingo.commons.api.edm.provider.annotation.CsdlIf;
import org.apache.olingo.commons.api.edm.provider.annotation.CsdlIsOf;
import org.apache.olingo.commons.api.edm.provider.annotation.CsdlLogicalOrComparisonExpression;
import org.apache.olingo.commons.api.edm.provider.annotation.CsdlPropertyValue;
import org.apache.olingo.commons.api.edm.provider.annotation.CsdlUrlRef;

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
public class SapODataSource extends BatchSource<NullWritable, ODataEntity, StructuredRecord> {

  private final SapODataConfig config;
  private ODataEntryToRecordTransformer transformer;

  public SapODataSource(SapODataConfig config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
    FailureCollector collector = stageConfigurer.getFailureCollector();
    config.validate(collector);
    try {
      // API call validation
      new GenericODataClient(config.getUrl(), config.getUser(), config.getPassword())
        .getEntitySetType(config.getResourcePath());
    } catch (ODataException e) {
      collector.addFailure("Unable to connect to OData Service: " + e.getMessage(), null)
        .withStacktrace(e.getStackTrace());
      collector.getOrThrowException();
    }

    Schema schema = getSchema();
    Schema configuredSchema = config.getParsedSchema();
    if (configuredSchema == null) {
      pipelineConfigurer.getStageConfigurer().setOutputSchema(schema);
      return;
    }

    SapODataConfig.validateFieldsMatch(schema, configuredSchema, collector);
    pipelineConfigurer.getStageConfigurer().setOutputSchema(configuredSchema);
  }

  @Override
  public void prepareRun(BatchSourceContext context) {
    FailureCollector collector = context.getFailureCollector();
    config.validate(collector);
    try {
      // API call validation
      new GenericODataClient(config.getUrl(), config.getUser(), config.getPassword())
        .getEntitySetType(config.getResourcePath());
    } catch (ODataException e) {
      collector.addFailure("Unable to connect to OData Service: " + e.getMessage(), null)
        .withStacktrace(e.getStackTrace());
      collector.getOrThrowException();
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
        .map(propertyMetadata -> getSchemaField(propertyMetadata, config.isIncludeMetadataAnnotations()))
        .collect(Collectors.toList());
      return Schema.recordOf("output", fields);
    } catch (ODataException e) {
      throw new InvalidStageException("Unable to get details about the entity type: " + e.getMessage(), e);
    }
  }

  private Map<String, List<ODataAnnotation>> getMetadataAnnotations() {
    GenericODataClient oDataClient = new GenericODataClient(config.getUrl(), config.getUser(), config.getPassword());
    try {
      EntityType entityType = oDataClient.getEntitySetType(config.getResourcePath());
      return entityType.getProperties().stream()
        .filter(p -> config.getSelectProperties().isEmpty() || config.getSelectProperties().contains(p.getName()))
        .collect(HashMap::new, (m, v) -> m.put(v.getName(), v.getAnnotations()), HashMap::putAll);
    } catch (ODataException e) {
      throw new InvalidStageException("Unable to get metadata annotations for the entity type: " + e.getMessage(), e);
    }
  }

  private Schema.Field getSchemaField(PropertyMetadata propertyMetadata, boolean includeAnnotations) {
    Schema nonNullableSchema = convertPropertyType(propertyMetadata);
    Schema schema = propertyMetadata.isNullable() ? Schema.nullableOf(nonNullableSchema) : nonNullableSchema;

    return includeAnnotations && propertyMetadata.getAnnotations() != null ?
      getFieldWithAnnotations(propertyMetadata, schema) : Schema.Field.of(propertyMetadata.getName(), schema);
  }

  private Schema.Field getFieldWithAnnotations(PropertyMetadata propertyMetadata, Schema valueSchema) {
    String propertyName = propertyMetadata.getName();
    List<ODataAnnotation> annotations = propertyMetadata.getAnnotations();
    List<Schema.Field> fields = annotations.stream()
      .map(annotation -> Schema.Field.of(annotation.getName(), annotationToFieldSchema(propertyName, annotation)))
      .collect(Collectors.toList());

    Schema.Field metadataField = Schema.Field.of(SapODataConstants.METADATA_ANNOTATIONS_FIELD_NAME,
                                                 Schema.recordOf(propertyName + "-metadata-record", fields));

    Schema valueWithMetadataSchema = Schema.recordOf(propertyName + "-value-with-metadata-record",
                                                     Schema.Field.of(SapODataConstants.VALUE_FIELD_NAME, valueSchema),
                                                     metadataField);
    return Schema.Field.of(propertyName, valueWithMetadataSchema);
  }

  private Schema annotationToFieldSchema(String fieldName, ODataAnnotation oDataAnnotation) {
    if (oDataAnnotation instanceof OData2Annotation) {
      // OData 2 annotations are strings
      return Schema.of(Schema.Type.STRING);
    }

    // OData 4 annotations
    OData4Annotation annotation = (OData4Annotation) oDataAnnotation;
    CsdlExpression expression = annotation.getExpression();
    Schema expressionSchema = expressionToFieldSchema(fieldName, expression);
    return SapODataSchemas.annotationSchema(fieldName, expressionSchema);
  }

  // TODO nested annotations
  private Schema expressionToFieldSchema(String fieldName, CsdlExpression expression) {
    if (expression == null) {
      return SapODataSchemas.singleValueExpressionSchema(fieldName);
    }
    if (expression.isConstant()) {
      return SapODataSchemas.singleValueExpressionSchema(fieldName);
    }
    CsdlDynamicExpression dynamic = expression.asDynamic();
    if (dynamic.isPropertyPath() || dynamic.isAnnotationPath() || dynamic.isLabeledElementReference()
      || dynamic.isNavigationPropertyPath() || dynamic.isPropertyPath() || dynamic.isNull() || dynamic.isPath()) {
      return SapODataSchemas.singleValueExpressionSchema(fieldName);
    }
    if (dynamic.isApply()) {
      List<Schema.Field> parameters = dynamic.asApply().getParameters().stream()
        .map(e -> Schema.Field.of(e.toString(), expressionToFieldSchema(fieldName, e))) // TODO review to String
        .collect(Collectors.toList());
      Schema parametersSchema = Schema.recordOf(fieldName + "-parameters", parameters);
      return SapODataSchemas.applyExpressionSchema(fieldName, parametersSchema);
    }
    if (dynamic.isCast()) {
      Schema expressionSchema = expressionToFieldSchema(fieldName, dynamic.asCast().getValue());
      return SapODataSchemas.castExpressionSchema(fieldName, expressionSchema);
    }
    if (dynamic.isCollection()) {
      List<CsdlExpression> items = dynamic.asCollection().getItems();
      // The values of the child expressions MUST all be type compatible.
      Schema itemSchema = items == null || items.isEmpty() ? null : expressionToFieldSchema(fieldName, items.get(0));
      return SapODataSchemas.collectionExpressionSchema(fieldName, itemSchema);
    }
    if (dynamic.isIf()) {
      CsdlIf csdlIf = dynamic.asIf();
      Schema guard = expressionToFieldSchema(fieldName, csdlIf.getGuard());
      Schema then = expressionToFieldSchema(fieldName, csdlIf.getThen());
      Schema elseSchema = expressionToFieldSchema(fieldName, csdlIf.getElse());
      return SapODataSchemas.ifExpressionSchema(fieldName, guard, then, elseSchema);
    }
    if (dynamic.isIsOf()) {
      CsdlIsOf isOf = dynamic.asIsOf();
      return SapODataSchemas.isOfExpressionSchema(fieldName, expressionToFieldSchema(fieldName, isOf.getValue()));
    }
    if (dynamic.isLabeledElement()) {
      Schema labeledElementSchema = expressionToFieldSchema(fieldName, dynamic.asLabeledElement().getValue());
      return SapODataSchemas.labeledElementExpressionSchema(fieldName, labeledElementSchema);
    }
    if (dynamic.isRecord()) {
      Schema propertyValuesSchema = propertyValuesSchema(fieldName, dynamic.asRecord().getPropertyValues());
      return SapODataSchemas.recordExpressionSchema(fieldName, propertyValuesSchema);
    }
    if (dynamic.isUrlRef()) {
      CsdlUrlRef urlRef = dynamic.asUrlRef();
      return SapODataSchemas.urlRefExpressionSchema(fieldName, expressionToFieldSchema(fieldName, urlRef.getValue()));
    }
    // Expression can only be a logical at this point
    CsdlLogicalOrComparisonExpression logical = dynamic.asLogicalOrComparison();
    if (logical.getType() == CsdlLogicalOrComparisonExpression.LogicalOrComparisonExpressionType.Not) {
      // Negation expressions are represented as an element edm:Not that MUST contain a single annotation expression.
      // See 'Comparison and Logical Operators' section of
      // https://docs.oasis-open.org/odata/odata-csdl-xml/v4.01/csprd05/odata-csdl-xml-v4.01-csprd05.html
      // However, Olingo's EdmNot interface extends common interface for logical or comparison expressions.
      // Thus, value expression can be accessed via either 'getLeftExpression' or 'getRightExpression'.
      // See: AbstractEdmLogicalOrComparisonExpression#getRightExpression implementation for details
      Schema value = expressionToFieldSchema(fieldName, logical.getLeft());
      return SapODataSchemas.notExpressionSchema(fieldName + "-not", value);
    }

    Schema andLeft = expressionToFieldSchema(fieldName, logical.getLeft());
    Schema andRight = expressionToFieldSchema(fieldName, logical.getRight());
    return SapODataSchemas.logicalExpressionSchema(fieldName + "-and", andLeft, andRight);
  }

  private Schema propertyValuesSchema(String fieldName, List<CsdlPropertyValue> propertyValues) {
    if (propertyValues == null || propertyValues.isEmpty()) {
      return null;
    }
    List<Schema.Field> fields = propertyValues.stream()
      .map(p -> Schema.Field.of(p.getProperty(), expressionToFieldSchema(fieldName, p.getValue())))
      .collect(Collectors.toList());

    return Schema.recordOf(fieldName + "-property-values", fields);
  }

  private Schema convertPropertyType(PropertyMetadata propertyMetadata) {
    switch (propertyMetadata.getEdmTypeName()) {
      case "Binary":
      case "Edm.Binary":
        return Schema.of(Schema.Type.BYTES);
      case "Boolean":
      case "Edm.Boolean":
        return Schema.of(Schema.Type.BOOLEAN);
      case "Byte":
      case "Edm.Byte":
        return Schema.of(Schema.Type.INT);
      case "SByte":
      case "Edm.SByte":
        return Schema.of(Schema.Type.INT);
      case "DateTime":
      case "Edm.DateTime":
        return Schema.of(Schema.LogicalType.TIMESTAMP_MICROS);
      case "DateTimeOffset":
      case "Edm.DateTimeOffset":
        // Mapped to 'string' to avoid timezone information loss
        return Schema.of(Schema.Type.STRING);
      case "Time":
      case "Edm.Time":
        return Schema.of(Schema.LogicalType.TIME_MICROS);
      case "Decimal":
      case "Edm.Decimal":
        return Schema.decimalOf(propertyMetadata.getPrecision(), propertyMetadata.getScale());
      case "Double":
      case "Edm.Double":
        return Schema.of(Schema.Type.DOUBLE);
      case "Single":
      case "Edm.Single":
        return Schema.of(Schema.Type.FLOAT);
      case "Guid":
      case "Edm.Guid":
        return Schema.of(Schema.Type.STRING);
      case "Int16":
      case "Edm.Int16":
        return Schema.of(Schema.Type.INT);
      case "Int32":
      case "Edm.Int32":
        return Schema.of(Schema.Type.INT);
      case "Int64":
      case "Edm.Int64":
        return Schema.of(Schema.Type.LONG);
      case "String":
      case "Edm.String":
        return Schema.of(Schema.Type.STRING);
      case "GeographyPoint":
      case "Edm.GeographyPoint":
      case "GeometryPoint":
      case "Edm.GeometryPoint":
        return SapODataSchemas.pointSchema(propertyMetadata.getName());
      case "GeographyLineString":
      case "Edm.GeographyLineString":
      case "GeometryLineString":
      case "Edm.GeometryLineString":
        return SapODataSchemas.lineStringSchema(propertyMetadata.getName());
      case "GeographyPolygon":
      case "Edm.GeographyPolygon":
      case "GeometryPolygon":
      case "Edm.GeometryPolygon":
        return SapODataSchemas.polygonSchema(propertyMetadata.getName());
      case "GeographyMultiPoint":
      case "Edm.GeographyMultiPoint":
      case "GeometryMultiPoint":
      case "Edm.GeometryMultiPoint":
        return SapODataSchemas.multiPointSchema(propertyMetadata.getName());
      case "GeographyMultiLineString":
      case "Edm.GeographyMultiLineString":
      case "GeometryMultiLineString":
      case "Edm.GeometryMultiLineString":
        return SapODataSchemas.multiLineStringSchema(propertyMetadata.getName());
      case "GeographyMultiPolygon":
      case "Edm.GeographyMultiPolygon":
      case "GeometryMultiPolygon":
      case "Edm.GeometryMultiPolygon":
        return SapODataSchemas.multiPolygonSchema(propertyMetadata.getName());
      case "GeographyCollection":
      case "Edm.GeographyCollection":
      case "GeometryCollection":
      case "Edm.GeometryCollection":
        return SapODataSchemas.collectionSchema(propertyMetadata.getName());
      case "Date":
      case "Edm.Date":
        return Schema.of(Schema.LogicalType.TIMESTAMP_MICROS);
      case "Duration":
      case "Edm.Duration":
        return Schema.of(Schema.Type.STRING);
      case "Stream":
      case "Edm.Stream":
        return SapODataSchemas.streamSchema(propertyMetadata.getName());
      case "TimeOfDay":
      case "Edm.TimeOfDay":
        return Schema.of(Schema.LogicalType.TIME_MICROS);
      default:
        // this should never happen
        throw new InvalidStageException(String.format("Field '%s' is of unsupported type '%s'.",
                                                      propertyMetadata.getName(), propertyMetadata.getEdmTypeName()));
    }
  }
}
