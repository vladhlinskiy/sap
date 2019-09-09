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
import io.cdap.cdap.etl.mock.test.HydratorTestBase;

public abstract class SapODataTestBase extends HydratorTestBase {

  protected static Schema streamSchema(String name) {
    return Schema.recordOf(name + "-stream-record",
                           Schema.Field.of(SapODataConstants.STREAM_ETAG_FIELD_NAME,
                                           Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                           Schema.Field.of(SapODataConstants.STREAM_CONTENT_TYPE_FIELD_NAME,
                                           Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                           Schema.Field.of(SapODataConstants.STREAM_READ_LINK_FIELD_NAME,
                                           Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                           Schema.Field.of(SapODataConstants.STREAM_EDIT_LINK_FIELD_NAME,
                                           Schema.nullableOf(Schema.of(Schema.Type.STRING))));
  }

  protected static Schema pointSchema(String name) {
    return Schema.recordOf(name + "-point-record",
                           Schema.Field.of(SapODataConstants.GEOSPATIAL_TYPE_FIELD_NAME, Schema.of(Schema.Type.STRING)),
                           Schema.Field.of(SapODataConstants.GEOSPATIAL_COORDINATES_FIELD_NAME,
                                           Schema.arrayOf(Schema.of(Schema.Type.DOUBLE))));
  }

  protected static Schema lineStringSchema(String name) {
    return Schema.recordOf(name + "-line-string-record",
                           Schema.Field.of(SapODataConstants.GEOSPATIAL_TYPE_FIELD_NAME, Schema.of(Schema.Type.STRING)),
                           Schema.Field.of(SapODataConstants.GEOSPATIAL_COORDINATES_FIELD_NAME,
                                           Schema.arrayOf(Schema.arrayOf(Schema.of(Schema.Type.DOUBLE)))));
  }

  protected static Schema polygonSchema(String name) {
    return Schema.recordOf(
      name + "-polygon-record",
      Schema.Field.of(SapODataConstants.GEOSPATIAL_TYPE_FIELD_NAME, Schema.of(Schema.Type.STRING)),
      Schema.Field.of(SapODataConstants.GEOSPATIAL_COORDINATES_FIELD_NAME,
                      Schema.arrayOf(Schema.arrayOf(Schema.arrayOf(Schema.of(Schema.Type.DOUBLE))))));
  }

  protected static Schema multiPointSchema(String name) {
    return Schema.recordOf(
      name + "-multi-point-record",
      Schema.Field.of(SapODataConstants.GEOSPATIAL_TYPE_FIELD_NAME, Schema.of(Schema.Type.STRING)),
      Schema.Field.of(SapODataConstants.GEOSPATIAL_COORDINATES_FIELD_NAME,
                      Schema.arrayOf(Schema.arrayOf(Schema.of(Schema.Type.DOUBLE)))));
  }

  protected static Schema multiLineStringSchema(String name) {
    return Schema.recordOf(
      name + "-multi-line-string-record",
      Schema.Field.of(SapODataConstants.GEOSPATIAL_TYPE_FIELD_NAME, Schema.of(Schema.Type.STRING)),
      Schema.Field.of(SapODataConstants.GEOSPATIAL_COORDINATES_FIELD_NAME,
                      Schema.arrayOf(Schema.arrayOf(Schema.arrayOf(Schema.of(Schema.Type.DOUBLE))))));
  }

  protected static Schema multiPolygonSchema(String name) {
    return Schema.recordOf(
      name + "-multi-polygon-record",
      Schema.Field.of(SapODataConstants.GEOSPATIAL_TYPE_FIELD_NAME, Schema.of(Schema.Type.STRING)),
      Schema.Field.of(SapODataConstants.GEOSPATIAL_COORDINATES_FIELD_NAME,
                      Schema.arrayOf(Schema.arrayOf(Schema.arrayOf(Schema.arrayOf(Schema.of(Schema.Type.DOUBLE)))))));
  }

  protected static Schema collectionSchema(String name) {
    return Schema.recordOf(
      name + "-collection-record",
      Schema.Field.of(SapODataConstants.GEOSPATIAL_TYPE_FIELD_NAME, Schema.of(Schema.Type.STRING)),
      Schema.Field.of(SapODataConstants.GEO_COLLECTION_POINTS_FIELD_NAME, Schema.arrayOf(pointSchema(name))),
      Schema.Field.of(SapODataConstants.GEO_COLLECTION_LINE_STRINGS_FIELD_NAME, Schema.arrayOf(lineStringSchema(name))),
      Schema.Field.of(SapODataConstants.GEO_COLLECTION_POLYGONS_FIELD_NAME, Schema.arrayOf(polygonSchema(name))),
      Schema.Field.of(SapODataConstants.GEO_COLLECTION_MULTI_POINTS_FIELD_NAME, Schema.arrayOf(polygonSchema(name))),
      Schema.Field.of(SapODataConstants.GEO_COLLECTION_MULTI_POLYGONS_FIELD_NAME, Schema.arrayOf(polygonSchema(name))),
      Schema.Field.of(SapODataConstants.GEO_COLLECTION_MULTI_LINE_STRINGS_FIELD_NAME,
                      Schema.arrayOf(polygonSchema(name)))
      // nested collections can not be supported since metadata does not contain component info
    );
  }
}
