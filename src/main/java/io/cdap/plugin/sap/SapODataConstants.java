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

/**
 * SAP OData constants.
 */
public class SapODataConstants {

  private SapODataConstants() {
    throw new AssertionError("Should not instantiate static utility class.");
  }

  /**
   * SAP plugin name.
   */
  public static final String PLUGIN_NAME = "SapOData";

  /**
   * Configuration property name used to specify URL of the SAP OData service.
   */
  public static final String ODATA_SERVICE_URL = "url";

  /**
   * Configuration property name used to specify path of the SAP OData entity.
   */
  public static final String RESOURCE_PATH = "resourcePath";

  /**
   * Configuration property name used to specify OData query options to filter the data.
   */
  public static final String QUERY = "query";

  /**
   * Configuration property name used to specify username for basic authentication.
   */
  public static final String USERNAME = "username";

  /**
   * Configuration property name used to specify password for basic authentication.
   */
  public static final String PASSWORD = "password";

  /**
   * Configuration property name used to specify the schema of the entries.
   */
  public static final String SCHEMA = "schema";

  /**
   * Configuration property name used to specify whether the plugin should read metadata annotations and include them
   * to each CDAP record.
   */
  public static final String INCLUDE_METADATA_ANNOTATIONS = "includeMetadataAnnotations";

  /**
   * When metadata annotations are included(using {@link SapODataConstants#INCLUDE_METADATA_ANNOTATIONS}), each
   * property is mapped to a CDAP 'record' with exactly two fields: "{@value SapODataConstants#VALUE_FIELD_NAME}" for
   * value and "{@value SapODataConstants#METADATA_ANNOTATIONS_FIELD_NAME}" for metadata annotations.
   */
  public static final String VALUE_FIELD_NAME = "value";

  /**
   * When metadata annotations are included(using {@link SapODataConstants#INCLUDE_METADATA_ANNOTATIONS}), each
   * property is mapped to a CDAP 'record' with exactly two fields: "{@value SapODataConstants#VALUE_FIELD_NAME}" for
   * value and "{@value SapODataConstants#METADATA_ANNOTATIONS_FIELD_NAME}" for metadata annotations.
   */
  public static final String METADATA_ANNOTATIONS_FIELD_NAME = "metadata-annotations";

  /**
   * OData 4 geospatial data types are mapped to CDAP record with fields
   * "{@value SapODataConstants#GEOSPATIAL_TYPE_FIELD_NAME}" for type. For instance, "LineString"
   * and "MultiPoint" schemas are the same. Type name is required to distinguish them.
   */
  public static final String GEOSPATIAL_TYPE_FIELD_NAME = "type";

  /**
   * OData 4 geospatial data types are mapped to CDAP record with fields
   * "{@value SapODataConstants#GEOSPATIAL_DIMENSION_FIELD_NAME}" for dimension.
   */
  public static final String GEOSPATIAL_DIMENSION_FIELD_NAME = "dimension";

  /**
   * OData 4 geospatial data types are mapped to CDAP record with fields
   * "{@value SapODataConstants#GEOSPATIAL_TYPE_FIELD_NAME}" for type and
   * "{@value SapODataConstants#GEOSPATIAL_COORDINATES_FIELD_NAME}" for list of coordinates.
   */
  public static final String GEOSPATIAL_COORDINATES_FIELD_NAME = "coordinates";

  /**
   * OData 4 geospatial "Point" is mapped to CDAP record with
   * "{@value SapODataConstants#POINT_X_FIELD_NAME}",
   * "{@value SapODataConstants#POINT_Y_FIELD_NAME}" and
   * "{@value SapODataConstants#POINT_Z_FIELD_NAME}" fields for coordinates.
   */
  public static final String POINT_X_FIELD_NAME = "x";

  /**
   * OData 4 geospatial "Point" is mapped to CDAP record with
   * "{@value SapODataConstants#POINT_X_FIELD_NAME}",
   * "{@value SapODataConstants#POINT_Y_FIELD_NAME}" and
   * "{@value SapODataConstants#POINT_Z_FIELD_NAME}" fields for coordinates.
   */
  public static final String POINT_Y_FIELD_NAME = "y";

  /**
   * OData 4 geospatial "Point" is mapped to CDAP record with
   * "{@value SapODataConstants#POINT_X_FIELD_NAME}",
   * "{@value SapODataConstants#POINT_Y_FIELD_NAME}" and
   * "{@value SapODataConstants#POINT_Z_FIELD_NAME}" fields for coordinates.
   */
  public static final String POINT_Z_FIELD_NAME = "z";

  /**
   * OData 4 geospatial "Polygon" is mapped to CDAP record with
   * "{@value SapODataConstants#POLYGON_EXTERIOR_FIELD_NAME}" field for the exterior coordinates.
   */
  public static final String POLYGON_EXTERIOR_FIELD_NAME = "exterior";

  /**
   * OData 4 geospatial "Polygon" is mapped to CDAP record with
   * "{@value SapODataConstants#POLYGON_INTERIOR_FIELD_NAME}" field for the interior coordinates.
   */
  public static final String POLYGON_INTERIOR_FIELD_NAME = "interior";

  /**
   * OData 4 geospatial "Polygon" is mapped to CDAP record with
   * "{@value SapODataConstants#POLYGON_NUMBER_OF_INTERIOR_RINGS_FIELD_NAME}" field for the number of interior rings.
   */
  public static final String POLYGON_NUMBER_OF_INTERIOR_RINGS_FIELD_NAME = "numberOfInteriorRings";

  /**
   * OData 4 Stream type mapped to CDAP record with field
   * "{@value SapODataConstants#STREAM_ETAG_FIELD_NAME}" for the ETag of the stream.
   */
  public static final String STREAM_ETAG_FIELD_NAME = "mediaEtag";

  /**
   * OData 4 Stream type mapped to CDAP record with field
   * "{@value SapODataConstants#STREAM_ETAG_FIELD_NAME}" for the media type of the stream.
   */
  public static final String STREAM_CONTENT_TYPE_FIELD_NAME = "mediaContentType";

  /**
   * OData 4 Stream type mapped to CDAP record with field
   * "{@value SapODataConstants#STREAM_ETAG_FIELD_NAME}" for the link used to read the stream.
   */
  public static final String STREAM_READ_LINK_FIELD_NAME = "mediaReadLink";

  /**
   * OData 4 Stream type mapped to CDAP record with field
   * "{@value SapODataConstants#STREAM_ETAG_FIELD_NAME}" for the link used to edit/update the stream.
   */
  public static final String STREAM_EDIT_LINK_FIELD_NAME = "mediaEditLink";

  /**
   * OData 4 geospatial collection mapped to CDAP record with field
   * "{@value SapODataConstants#GEO_COLLECTION_POINTS_FIELD_NAME}" for a list of geospatial "Point" values.
   */
  public static final String GEO_COLLECTION_POINTS_FIELD_NAME = "points";

  /**
   * OData 4 geospatial collection mapped to CDAP record with field
   * "{@value SapODataConstants#GEO_COLLECTION_LINE_STRINGS_FIELD_NAME}" for a list of geospatial "LineString" values.
   */
  public static final String GEO_COLLECTION_LINE_STRINGS_FIELD_NAME = "lineStrings";

  /**
   * OData 4 geospatial collection mapped to CDAP record with field
   * "{@value SapODataConstants#GEO_COLLECTION_POLYGONS_FIELD_NAME}" for a list of geospatial "Polygon" values.
   */
  public static final String GEO_COLLECTION_POLYGONS_FIELD_NAME = "polygons";

  /**
   * OData 4 geospatial collection mapped to CDAP record with field
   * "{@value SapODataConstants#GEO_COLLECTION_MULTI_POINTS_FIELD_NAME}" for a list of geospatial "MultiPoint" values.
   */
  public static final String GEO_COLLECTION_MULTI_POINTS_FIELD_NAME = "multiPoints";

  /**
   * OData 4 geospatial collection mapped to CDAP record with field
   * "{@value SapODataConstants#GEO_COLLECTION_MULTI_LINE_STRINGS_FIELD_NAME}" for a list of geospatial
   * "MultiLineString" values.
   */
  public static final String GEO_COLLECTION_MULTI_LINE_STRINGS_FIELD_NAME = "multiLineStrings";

  /**
   * OData 4 geospatial collection mapped to CDAP record with field
   * "{@value SapODataConstants#GEO_COLLECTION_MULTI_POLYGONS_FIELD_NAME}" for a list of geospatial "MultiPolygon"
   * values.
   */
  public static final String GEO_COLLECTION_MULTI_POLYGONS_FIELD_NAME = "multiPolygons";

  /**
   * OData 4 metadata annotations mapped to CDAP record with field
   * "{@value SapODataConstants#ANNOTATION_TERM_FIELD_NAME}" for a term name.
   */
  public static final String ANNOTATION_TERM_FIELD_NAME = "term";

  /**
   * OData 4 metadata annotations mapped to CDAP record with field
   * "{@value SapODataConstants#ANNOTATION_QUALIFIER_FIELD_NAME}" for a qualifier name.
   */
  public static final String ANNOTATION_QUALIFIER_FIELD_NAME = "qualifier";

  /**
   * OData 4 metadata annotations mapped to CDAP record with field
   * "{@value SapODataConstants#ANNOTATION_EXPRESSION_FIELD_NAME}" for an expression record.
   */
  public static final String ANNOTATION_EXPRESSION_FIELD_NAME = "expression";

  /**
   * OData 4 metadata annotation expression mapped to CDAP record with field
   * "{@value SapODataConstants#NESTED_ANNOTATIONS_FIELD_NAME}" for a nested annotation record.
   */
  public static final String NESTED_ANNOTATIONS_FIELD_NAME = "annotations";

  /**
   * OData 4 metadata annotation expression mapped to CDAP record with field
   * "{@value SapODataConstants#EXPRESSION_NAME_FIELD_NAME}" for an expression name.
   */
  public static final String EXPRESSION_NAME_FIELD_NAME = "name";

  /**
   * Some of the OData 4 metadata annotation expression mapped to CDAP record with field
   * "{@value SapODataConstants#EXPRESSION_VALUE_FIELD_NAME}" for an expression value.
   * These expressions include:
   * - Constant expressions
   * - Path
   * - AnnotationPath
   * - LabeledElementReference
   * - Null
   * - NavigationPropertyPath
   * - PropertyPath
   * - Not
   * - Cast
   * - IsOf
   * - LabeledElement
   * - UrlRef
   */
  public static final String EXPRESSION_VALUE_FIELD_NAME = "value";

  /**
   * OData 4 "Apply" metadata annotation expression mapped to CDAP record with field
   * "{@value SapODataConstants#EXPRESSION_FUNCTION_FIELD_NAME}" for a function name.
   */
  public static final String EXPRESSION_FUNCTION_FIELD_NAME = "function";

  /**
   * OData 4 "Apply" metadata annotation expression mapped to CDAP record with field
   * "{@value SapODataConstants#EXPRESSION_FUNCTION_FIELD_NAME}" for a parameters record.
   */
  public static final String EXPRESSION_PARAMETERS_FIELD_NAME = "parameters";

  /**
   * OData 4 logical & comparison expressions mapped to CDAP record with field
   * "{@value SapODataConstants#EXPRESSION_LEFT_FIELD_NAME}" for a left expression record.
   */
  public static final String EXPRESSION_LEFT_FIELD_NAME = "left";

  /**
   * OData 4 logical & comparison expressions mapped to CDAP record with field
   * "{@value SapODataConstants#EXPRESSION_RIGHT_FIELD_NAME}" for a right expression record.
   */
  public static final String EXPRESSION_RIGHT_FIELD_NAME = "right";

  /**
   * Some of the OData 4 metadata annotation expression mapped to CDAP record with field
   * "{@value SapODataConstants#EXPRESSION_TYPE_FIELD_NAME}" for a type name.
   * These expressions include:
   * - Cast
   * - IsOf
   * - Record
   */
  public static final String EXPRESSION_TYPE_FIELD_NAME = "type";

  /**
   * OData 4 "Cast" and "IsOf" expressions mapped to CDAP record with field
   * "{@value SapODataConstants#EXPRESSION_MAX_LENGTH_FIELD_NAME}" for a maximum length of value.
   */
  public static final String EXPRESSION_MAX_LENGTH_FIELD_NAME = "maxLength";

  /**
   * OData 4 "Cast" and "IsOf" expressions mapped to CDAP record with field
   * "{@value SapODataConstants#EXPRESSION_PRECISION_FIELD_NAME}" for a precision of value.
   */
  public static final String EXPRESSION_PRECISION_FIELD_NAME = "precision";

  /**
   * OData 4 "Cast" and "IsOf" expressions mapped to CDAP record with field
   * "{@value SapODataConstants#EXPRESSION_SCALE_FIELD_NAME}" for a scale of value.
   */
  public static final String EXPRESSION_SCALE_FIELD_NAME = "scale";

  /**
   * OData 4 "Cast" and "IsOf" expressions mapped to CDAP record with field
   * "{@value SapODataConstants#EXPRESSION_SRID_FIELD_NAME}" for a SRID of value.
   */
  public static final String EXPRESSION_SRID_FIELD_NAME = "srid";

  /**
   * OData 4 "Collection" expression mapped to CDAP record with field
   * "{@value SapODataConstants#EXPRESSION_ITEMS_FIELD_NAME}" for an array of item expressions.
   */
  public static final String EXPRESSION_ITEMS_FIELD_NAME = "items";

  /**
   * OData 4 "If" expression mapped to CDAP record with field
   * "{@value SapODataConstants#EXPRESSION_GUARD_FIELD_NAME}" for a guard expression.
   */
  public static final String EXPRESSION_GUARD_FIELD_NAME = "guard";

  /**
   * OData 4 "If" expression mapped to CDAP record with field
   * "{@value SapODataConstants#EXPRESSION_THEN_FIELD_NAME}" for a then expression.
   */
  public static final String EXPRESSION_THEN_FIELD_NAME = "then";

  /**
   * OData 4 "If" expression mapped to CDAP record with field
   * "{@value SapODataConstants#EXPRESSION_ELSE_FIELD_NAME}" for an else expression.
   */
  public static final String EXPRESSION_ELSE_FIELD_NAME = "else";

  /**
   * OData 4 "LabeledElement" expression mapped to CDAP record with field
   * "{@value SapODataConstants#EXPRESSION_ELEMENT_NAME_FIELD_NAME}" for an element name.
   */
  public static final String EXPRESSION_ELEMENT_NAME_FIELD_NAME = "elementName";

  /**
   * OData 4 "Record" expression mapped to CDAP record with field
   * "{@value SapODataConstants#EXPRESSION_PROPERTY_VALUES_FIELD_NAME}" for a record of property values.
   */
  public static final String EXPRESSION_PROPERTY_VALUES_FIELD_NAME = "propertyValues";
}
