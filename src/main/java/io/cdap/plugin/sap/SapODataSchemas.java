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

import io.cdap.cdap.api.data.schema.Schema;

import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;

/**
 * Utility methods to construct SAP OData schemas.
 */
public class SapODataSchemas {

  private SapODataSchemas() {
    throw new AssertionError("Should not instantiate static utility class.");
  }


  public static Schema streamSchema(String name) {
    return Schema.recordOf(
      name + "-stream-record",
      Schema.Field.of(SapODataConstants.STREAM_ETAG_FIELD_NAME, Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of(SapODataConstants.STREAM_CONTENT_TYPE_FIELD_NAME,
                      Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of(SapODataConstants.STREAM_READ_LINK_FIELD_NAME, Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of(SapODataConstants.STREAM_EDIT_LINK_FIELD_NAME, Schema.nullableOf(Schema.of(Schema.Type.STRING))));
  }

  public static Schema pointSchema(String name) {
    return Schema.recordOf(name + "-point-record",
                           Schema.Field.of(SapODataConstants.GEOSPATIAL_TYPE_FIELD_NAME, Schema.of(Schema.Type.STRING)),
                           Schema.Field.of(SapODataConstants.GEOSPATIAL_COORDINATES_FIELD_NAME,
                                           Schema.arrayOf(Schema.of(Schema.Type.DOUBLE))));
  }

  public static Schema lineStringSchema(String name) {
    return Schema.recordOf(name + "-line-string-record",
                           Schema.Field.of(SapODataConstants.GEOSPATIAL_TYPE_FIELD_NAME, Schema.of(Schema.Type.STRING)),
                           Schema.Field.of(SapODataConstants.GEOSPATIAL_COORDINATES_FIELD_NAME,
                                           Schema.arrayOf(Schema.arrayOf(Schema.of(Schema.Type.DOUBLE)))));
  }

  public static Schema polygonSchema(String name) {
    return Schema.recordOf(
      name + "-polygon-record",
      Schema.Field.of(SapODataConstants.GEOSPATIAL_TYPE_FIELD_NAME, Schema.of(Schema.Type.STRING)),
      Schema.Field.of(SapODataConstants.GEOSPATIAL_COORDINATES_FIELD_NAME,
                      Schema.arrayOf(Schema.arrayOf(Schema.arrayOf(Schema.of(Schema.Type.DOUBLE))))));
  }

  public static Schema multiPointSchema(String name) {
    return Schema.recordOf(
      name + "-multi-point-record",
      Schema.Field.of(SapODataConstants.GEOSPATIAL_TYPE_FIELD_NAME, Schema.of(Schema.Type.STRING)),
      Schema.Field.of(SapODataConstants.GEOSPATIAL_COORDINATES_FIELD_NAME,
                      Schema.arrayOf(Schema.arrayOf(Schema.of(Schema.Type.DOUBLE)))));
  }

  public static Schema multiLineStringSchema(String name) {
    return Schema.recordOf(
      name + "-multi-line-string-record",
      Schema.Field.of(SapODataConstants.GEOSPATIAL_TYPE_FIELD_NAME, Schema.of(Schema.Type.STRING)),
      Schema.Field.of(SapODataConstants.GEOSPATIAL_COORDINATES_FIELD_NAME,
                      Schema.arrayOf(Schema.arrayOf(Schema.arrayOf(Schema.of(Schema.Type.DOUBLE))))));
  }

  public static Schema multiPolygonSchema(String name) {
    return Schema.recordOf(
      name + "-multi-polygon-record",
      Schema.Field.of(SapODataConstants.GEOSPATIAL_TYPE_FIELD_NAME, Schema.of(Schema.Type.STRING)),
      Schema.Field.of(SapODataConstants.GEOSPATIAL_COORDINATES_FIELD_NAME,
                      Schema.arrayOf(Schema.arrayOf(Schema.arrayOf(Schema.arrayOf(Schema.of(Schema.Type.DOUBLE)))))));
  }

  public static Schema collectionSchema(String name) {
    return Schema.recordOf(name + "-collection-record",
                           Schema.Field.of(SapODataConstants.GEOSPATIAL_TYPE_FIELD_NAME, Schema.of(Schema.Type.STRING)),
                           Schema.Field.of(SapODataConstants.GEO_COLLECTION_POINTS_FIELD_NAME,
                                           Schema.arrayOf(pointSchema(name))),
                           Schema.Field.of(SapODataConstants.GEO_COLLECTION_LINE_STRINGS_FIELD_NAME,
                                           Schema.arrayOf(lineStringSchema(name))),
                           Schema.Field.of(SapODataConstants.GEO_COLLECTION_POLYGONS_FIELD_NAME,
                                           Schema.arrayOf(polygonSchema(name))),
                           Schema.Field.of(SapODataConstants.GEO_COLLECTION_MULTI_POINTS_FIELD_NAME,
                                           Schema.arrayOf(multiPointSchema(name))),
                           Schema.Field.of(SapODataConstants.GEO_COLLECTION_MULTI_LINE_STRINGS_FIELD_NAME,
                                           Schema.arrayOf(multiLineStringSchema(name))),
                           Schema.Field.of(SapODataConstants.GEO_COLLECTION_MULTI_POLYGONS_FIELD_NAME,
                                           Schema.arrayOf(multiPolygonSchema(name)))
                           // nested collections can not be supported since metadata does not contain component info
    );
  }

  public static Schema annotationSchema(String name, Schema expressionSchema) {
    return Schema.recordOf(
      name + "-annotation",
      Schema.Field.of(SapODataConstants.ANNOTATION_TERM_FIELD_NAME, Schema.of(Schema.Type.STRING)),
      Schema.Field.of(SapODataConstants.ANNOTATION_QUALIFIER_FIELD_NAME,
                      Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of(SapODataConstants.ANNOTATION_EXPRESSION_FIELD_NAME, expressionSchema));
  }

  public static Schema constantExpressionSchema(String name) {
    return Schema.recordOf(
      name + "-expression",
      Schema.Field.of(SapODataConstants.EXPRESSION_NAME_FIELD_NAME, Schema.of(Schema.Type.STRING)),
      Schema.Field.of(SapODataConstants.EXPRESSION_VALUE_FIELD_NAME, Schema.nullableOf(Schema.of(Schema.Type.STRING))));
  }

  // TODO fix
  public static Schema applyExpressionSchema(String name, Schema parametersSchema) {
    return Schema.recordOf(
      name + "-apply-expression",
      Schema.Field.of(SapODataConstants.EXPRESSION_NAME_FIELD_NAME, Schema.of(Schema.Type.STRING)),
      Schema.Field.of(SapODataConstants.EXPRESSION_FUNCTION_FIELD_NAME,
                      Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of(SapODataConstants.EXPRESSION_PARAMETERS_FIELD_NAME, parametersSchema));
  }

  public static Schema logicalExpressionSchema(String name, Schema left, Schema right) {
    return Schema.recordOf(
      name + "-logical-expression",
      Schema.Field.of(SapODataConstants.EXPRESSION_NAME_FIELD_NAME, Schema.of(Schema.Type.STRING)),
      Schema.Field.of(SapODataConstants.EXPRESSION_LEFT_FIELD_NAME, left),
      Schema.Field.of(SapODataConstants.EXPRESSION_RIGHT_FIELD_NAME, right));
  }

  public static Schema notExpressionSchema(String name, Schema value) {
    return Schema.recordOf(
      name + "-not-expression",
      Schema.Field.of(SapODataConstants.EXPRESSION_NAME_FIELD_NAME, Schema.of(Schema.Type.STRING)),
      Schema.Field.of(SapODataConstants.EXPRESSION_VALUE_FIELD_NAME, value));
  }

  public static Schema castExpressionSchema(String name, Schema value) {
    return Schema.recordOf(
      name + "-cast-expression",
      Schema.Field.of(SapODataConstants.EXPRESSION_NAME_FIELD_NAME, Schema.of(Schema.Type.STRING)),
      Schema.Field.of(SapODataConstants.EXPRESSION_TYPE_FIELD_NAME, Schema.of(Schema.Type.STRING)),
      Schema.Field.of(SapODataConstants.EXPRESSION_MAX_LENGTH_FIELD_NAME,
                      Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of(SapODataConstants.EXPRESSION_PRECISION_FIELD_NAME,
                      Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of(SapODataConstants.EXPRESSION_SCALE_FIELD_NAME, Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of(SapODataConstants.EXPRESSION_SRID_FIELD_NAME, Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of(SapODataConstants.EXPRESSION_VALUE_FIELD_NAME, value));
  }

  /**
   * The values of the child expressions MUST all be type compatible.
   * https://docs.oasis-open.org/odata/odata-csdl-xml/v4.01/csprd05/odata-csdl-xml-v4.01-csprd05.html#sec_Collection
   *
   * @param name
   * @param componentSchema
   * @return
   */
  public static Schema collectionExpressionSchema(String name, @Nullable Schema componentSchema) {
    List<Schema.Field> fields = new ArrayList<>();
    fields.add(Schema.Field.of(SapODataConstants.EXPRESSION_NAME_FIELD_NAME, Schema.of(Schema.Type.STRING)));
    if (componentSchema != null) {
      fields.add(Schema.Field.of(SapODataConstants.EXPRESSION_ITEMS_FIELD_NAME, Schema.arrayOf(componentSchema)));
    }

    return Schema.recordOf(name + "-collection-expression", fields);
  }

  public static Schema ifExpressionSchema(String name, Schema guard, Schema then, Schema elseSchema) {
    return Schema.recordOf(
      name + "-if-expression",
      Schema.Field.of(SapODataConstants.EXPRESSION_NAME_FIELD_NAME, Schema.of(Schema.Type.STRING)),
      Schema.Field.of(SapODataConstants.EXPRESSION_GUARD_FIELD_NAME, guard),
      Schema.Field.of(SapODataConstants.EXPRESSION_THEN_FIELD_NAME, then),
      Schema.Field.of(SapODataConstants.EXPRESSION_ELSE_FIELD_NAME, elseSchema));
  }

  public static Schema isOfExpressionSchema(String name, Schema value) {
    return Schema.recordOf(
      name + "-is-of-expression",
      Schema.Field.of(SapODataConstants.EXPRESSION_NAME_FIELD_NAME, Schema.of(Schema.Type.STRING)),
      Schema.Field.of(SapODataConstants.EXPRESSION_TYPE_FIELD_NAME, Schema.of(Schema.Type.STRING)),
      Schema.Field.of(SapODataConstants.EXPRESSION_MAX_LENGTH_FIELD_NAME,
                      Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of(SapODataConstants.EXPRESSION_PRECISION_FIELD_NAME,
                      Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of(SapODataConstants.EXPRESSION_SCALE_FIELD_NAME, Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of(SapODataConstants.EXPRESSION_SRID_FIELD_NAME, Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of(SapODataConstants.EXPRESSION_VALUE_FIELD_NAME, value));
  }

  public static Schema labeledElementExpressionSchema(String name, Schema value) {
    return Schema.recordOf(
      name + "-labeled-element-expression",
      Schema.Field.of(SapODataConstants.EXPRESSION_NAME_FIELD_NAME, Schema.of(Schema.Type.STRING)),
      Schema.Field.of(SapODataConstants.EXPRESSION_ELEMENT_NAME_FIELD_NAME, Schema.of(Schema.Type.STRING)),
      Schema.Field.of(SapODataConstants.EXPRESSION_VALUE_FIELD_NAME, value));
  }

  /**
   * The edm:Record element MAY contain the Type attribute and MAY contain edm:PropertyValue elements.
   *
   * @param name
   * @param propertyValuesSchema
   * @return
   */
  public static Schema recordExpressionSchema(String name, @Nullable Schema propertyValuesSchema) {
    List<Schema.Field> fields = new ArrayList<>();
    fields.add(Schema.Field.of(SapODataConstants.EXPRESSION_NAME_FIELD_NAME, Schema.of(Schema.Type.STRING)));
    fields.add(Schema.Field.of(SapODataConstants.EXPRESSION_TYPE_FIELD_NAME,
                               Schema.nullableOf(Schema.of(Schema.Type.STRING))));
    if (propertyValuesSchema != null) {
      fields.add(Schema.Field.of(SapODataConstants.EXPRESSION_PROPERTY_VALUES_FIELD_NAME, propertyValuesSchema));
    }

    return Schema.recordOf(name + "-record-expression", fields);
  }

  public static Schema urlRefExpressionSchema(String name, Schema value) {
    return Schema.recordOf(
      name + "-url-ref-expression",
      Schema.Field.of(SapODataConstants.EXPRESSION_NAME_FIELD_NAME, Schema.of(Schema.Type.STRING)),
      Schema.Field.of(SapODataConstants.EXPRESSION_VALUE_FIELD_NAME, value));
  }
}
