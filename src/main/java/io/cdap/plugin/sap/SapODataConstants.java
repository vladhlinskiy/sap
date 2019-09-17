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
   * "{@value SapODataConstants#GEOSPATIAL_TYPE_FIELD_NAME}" for type and
   * "{@value SapODataConstants#GEOSPATIAL_COORDINATES_FIELD_NAME}" for list of coordinates.
   */
  public static final String GEOSPATIAL_TYPE_FIELD_NAME = "type";

  /**
   * OData 4 geospatial data types are mapped to CDAP record with fields
   * "{@value SapODataConstants#GEOSPATIAL_TYPE_FIELD_NAME}" for type and
   * "{@value SapODataConstants#GEOSPATIAL_COORDINATES_FIELD_NAME}" for list of coordinates.
   */
  public static final String GEOSPATIAL_COORDINATES_FIELD_NAME = "coordinates";

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

  public static final String ANNOTATION_TERM_FIELD_NAME = "term";
  public static final String ANNOTATION_QUALIFIER_FIELD_NAME = "qualifier";
  public static final String ANNOTATION_EXPRESSION_FIELD_NAME = "expression";
  public static final String EXPRESSION_NAME_FIELD_NAME = "name";
  public static final String EXPRESSION_VALUE_FIELD_NAME = "value";
  public static final String EXPRESSION_FUNCTION_FIELD_NAME = "function";
  public static final String EXPRESSION_PARAMETERS_FIELD_NAME = "parameters";
  public static final String EXPRESSION_LEFT_FIELD_NAME = "left";
  public static final String EXPRESSION_RIGHT_FIELD_NAME = "right";
  public static final String EXPRESSION_TYPE_FIELD_NAME = "type";
  public static final String EXPRESSION_MAX_LENGTH_FIELD_NAME = "maxLength";
  public static final String EXPRESSION_PRECISION_FIELD_NAME = "precision";
  public static final String EXPRESSION_SCALE_FIELD_NAME = "scale";
  public static final String EXPRESSION_SRID_FIELD_NAME = "srid";
  public static final String EXPRESSION_ITEMS_FIELD_NAME = "items";
  public static final String EXPRESSION_GUARD_FIELD_NAME = "guard";
  public static final String EXPRESSION_THEN_FIELD_NAME = "then";
  public static final String EXPRESSION_ELSE_FIELD_NAME = "else";
  public static final String EXPRESSION_ELEMENT_NAME_FIELD_NAME = "elementName";
  public static final String EXPRESSION_PROPERTY_VALUES_FIELD_NAME = "propertyValues";
}
