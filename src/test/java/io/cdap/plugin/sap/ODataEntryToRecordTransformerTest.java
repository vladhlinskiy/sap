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

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.sap.odata.ODataEntity;
import io.cdap.plugin.sap.transformer.ODataEntryToRecordTransformer;
import org.apache.olingo.client.api.domain.ClientLink;
import org.apache.olingo.client.api.domain.ClientLinkType;
import org.apache.olingo.client.core.domain.ClientEntityImpl;
import org.apache.olingo.commons.api.Constants;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.apache.olingo.commons.api.edm.geo.Geospatial;
import org.apache.olingo.commons.api.edm.geo.GeospatialCollection;
import org.apache.olingo.commons.api.edm.geo.LineString;
import org.apache.olingo.commons.api.edm.geo.MultiLineString;
import org.apache.olingo.commons.api.edm.geo.MultiPoint;
import org.apache.olingo.commons.api.edm.geo.MultiPolygon;
import org.apache.olingo.commons.api.edm.geo.Point;
import org.apache.olingo.commons.api.edm.geo.Polygon;
import org.apache.olingo.commons.api.edm.geo.SRID;
import org.apache.olingo.odata2.api.edm.EdmSimpleTypeException;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.math.BigDecimal;
import java.net.URI;
import java.sql.Timestamp;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * {@link ODataEntryToRecordTransformer} test.
 */
public class ODataEntryToRecordTransformerTest extends SapODataTestBase {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  @SuppressWarnings("ConstantConditions")
  public void testTransformOData2Types() throws EdmSimpleTypeException {
    Schema schema = Schema.recordOf("schema",
                                    Schema.Field.of("binary", Schema.of(Schema.Type.BYTES)),
                                    Schema.Field.of("boolean", Schema.of(Schema.Type.BOOLEAN)),
                                    Schema.Field.of("byte", Schema.of(Schema.Type.INT)),
                                    Schema.Field.of("datetime", Schema.of(Schema.LogicalType.TIMESTAMP_MICROS)),
                                    Schema.Field.of("datetime_millis", Schema.of(Schema.LogicalType.TIMESTAMP_MILLIS)),
                                    Schema.Field.of("decimal", Schema.decimalOf(4, 2)),
                                    Schema.Field.of("double", Schema.of(Schema.Type.DOUBLE)),
                                    Schema.Field.of("single", Schema.of(Schema.Type.FLOAT)),
                                    Schema.Field.of("guid", Schema.of(Schema.Type.STRING)),
                                    Schema.Field.of("int16", Schema.of(Schema.Type.INT)),
                                    Schema.Field.of("int32", Schema.of(Schema.Type.INT)),
                                    Schema.Field.of("int64", Schema.of(Schema.Type.LONG)),
                                    Schema.Field.of("sbyte", Schema.of(Schema.Type.INT)),
                                    Schema.Field.of("string", Schema.of(Schema.Type.STRING)),
                                    Schema.Field.of("time", Schema.of(Schema.LogicalType.TIME_MICROS)),
                                    Schema.Field.of("time_millis", Schema.of(Schema.LogicalType.TIME_MILLIS)),
                                    Schema.Field.of("datetimeoffset", Schema.of(Schema.Type.STRING)),
                                    Schema.Field.of("null", Schema.nullableOf(Schema.of(Schema.Type.STRING))));

    LocalTime time = LocalTime.now(ZoneOffset.UTC);
    StructuredRecord expected = StructuredRecord.builder(schema)
      .set("binary", "some bytes".getBytes())
      .set("boolean", true)
      .set("byte", (int) Byte.MAX_VALUE)
      .setTimestamp("datetime", ZonedDateTime.now(ZoneOffset.UTC))
      .setTimestamp("datetime_millis", ZonedDateTime.now(ZoneOffset.UTC))
      .setDecimal("decimal", new BigDecimal("12.34"))
      .set("double", Double.MAX_VALUE)
      .set("single", Float.MAX_VALUE)
      .set("guid", UUID.randomUUID().toString())
      .set("int16", (int) Short.MAX_VALUE)
      .set("int32", Integer.MAX_VALUE)
      .set("int64", Long.MAX_VALUE)
      .set("sbyte", (int) Byte.MIN_VALUE)
      .set("string", "Some String")
      .setTime("time", time)
      .setTime("time_millis", time)
      .set("datetimeoffset", "2002-10-10T17:00:01+01:00")
      .set("null", null)
      .build();

    Calendar timeCalendar = Calendar.getInstance();
    timeCalendar.set(Calendar.HOUR_OF_DAY, time.getHour());
    timeCalendar.set(Calendar.MINUTE, time.getMinute());
    timeCalendar.set(Calendar.SECOND, time.getSecond());
    timeCalendar.set(Calendar.MILLISECOND, (int) TimeUnit.NANOSECONDS.toMillis(time.getNano()));
    ODataEntity entity = ODataEntityBuilder.builder()
      .setBinary("binary", expected.<byte[]>get("binary"))
      .setBoolean("boolean", expected.get("boolean"))
      .setByte("byte", expected.<Number>get("byte").byteValue())
      .setDateTime("datetime", GregorianCalendar.from(expected.getTimestamp("datetime")))
      .setDateTime("datetime_millis", GregorianCalendar.from(expected.getTimestamp("datetime_millis")))
      .setDecimal("decimal", expected.getDecimal("decimal"))
      .setDouble("double", expected.<Double>get("double"))
      .setSingle("single", expected.<Float>get("single"))
      .setGuid("guid", UUID.fromString(expected.get("guid")))
      .setInt16("int16", expected.<Number>get("int16").shortValue())
      .setInt32("int32", expected.<Integer>get("int32"))
      .setInt64("int64", expected.<Long>get("int64"))
      .setSByte("sbyte", expected.<Number>get("sbyte").byteValue())
      .setString("string", expected.get("string"))
      .setTime("time", timeCalendar)
      .setTime("time_millis", timeCalendar)
      .setDateTimeOffset("datetimeoffset", expected.<String>get("datetimeoffset"))
      .build();

    ODataEntryToRecordTransformer transformer = new ODataEntryToRecordTransformer(schema);
    StructuredRecord transformed = transformer.transform(entity);

    Assert.assertArrayEquals(expected.<byte[]>get("binary"), transformed.get("binary"));
    Assert.assertEquals(expected.<Boolean>get("boolean"), transformed.get("boolean"));
    Assert.assertEquals(expected.<Byte>get("byte"), transformed.get("byte"));
    Assert.assertEquals(expected.getTimestamp("datetime"), transformed.getTimestamp("datetime"));
    Assert.assertEquals(expected.getTimestamp("datetime_millis"), transformed.getTimestamp("datetime_millis"));
    Assert.assertEquals(expected.getDecimal("decimal"), transformed.getDecimal("decimal"));
    Assert.assertEquals(expected.<Double>get("double"), transformed.get("double"), 0.00001);
    Assert.assertEquals(expected.<Float>get("single"), transformed.<Float>get("single"), 0.00001);
    Assert.assertEquals(expected.<String>get("guid"), transformed.get("guid"));
    Assert.assertEquals(expected.<Short>get("int16"), transformed.get("int16"));
    Assert.assertEquals(expected.<Integer>get("int32"), transformed.get("int32"));
    Assert.assertEquals(expected.<Long>get("int64"), transformed.get("int64"));
    Assert.assertEquals(expected.<Byte>get("sbyte"), transformed.get("sbyte"));
    Assert.assertEquals(expected.<String>get("string"), transformed.get("string"));
    Assert.assertEquals(expected.getTime("time"), transformed.getTime("time"));
    Assert.assertEquals(expected.getTime("time_millis"), transformed.getTime("time_millis"));
    Assert.assertEquals(expected.<String>get("datetimeoffset"), transformed.get("datetimeoffset"));
    Assert.assertNull(transformed.get("null"));
  }

  @Test
  @SuppressWarnings("ConstantConditions")
  public void testTransformOData4SimpleTypes() throws Exception {

    Schema schema = Schema.recordOf("schema",
                                    Schema.Field.of("date_micros", Schema.of(Schema.LogicalType.TIMESTAMP_MICROS)),
                                    Schema.Field.of("date_millis", Schema.of(Schema.LogicalType.TIMESTAMP_MILLIS)),
                                    Schema.Field.of("duration", Schema.of(Schema.Type.STRING)),
                                    Schema.Field.of("time_of_day_micros", Schema.of(Schema.LogicalType.TIME_MICROS)),
                                    Schema.Field.of("time_of_day_millis", Schema.of(Schema.LogicalType.TIME_MILLIS)));

    LocalTime time = LocalTime.now(ZoneOffset.UTC);
    StructuredRecord expected = StructuredRecord.builder(schema)
      .setTimestamp("date_micros", ZonedDateTime.now(ZoneOffset.UTC))
      .setTimestamp("date_millis", ZonedDateTime.now(ZoneOffset.UTC))
      .set("duration", "P12DT23H59M59.999999999999S")
      .setTime("time_of_day_micros", time)
      .setTime("time_of_day_millis", time)
      .build();

    Timestamp timestamp = new Timestamp(0, 0, 0, time.getHour(), time.getMinute(), time.getSecond(), time.getNano());
    ODataEntity entity = ODataEntityBuilder.builder()
      .setDate("date_micros", Timestamp.from(expected.getTimestamp("date_micros").toInstant()))
      .setDate("date_millis", Timestamp.from(expected.getTimestamp("date_millis").toInstant()))
      .setDuration("duration", expected.get("duration"), 12, 19)
      .setTimeOfDay("time_of_day_micros", timestamp)
      .setTimeOfDay("time_of_day_millis", timestamp)
      .build();

    ODataEntryToRecordTransformer transformer = new ODataEntryToRecordTransformer(schema);
    StructuredRecord transformed = transformer.transform(entity);

    Assert.assertEquals(expected.getTimestamp("date_micros"), transformed.getTimestamp("date_micros"));
    Assert.assertEquals(expected.getTimestamp("date_millis"), transformed.getTimestamp("date_millis"));
    Assert.assertEquals(expected.<String>get("duration"), transformed.get("duration"));
    Assert.assertEquals(expected.getTime("time_of_day_micros"), transformed.getTime("time_of_day_micros"));
    Assert.assertEquals(expected.getTime("time_of_day_millis"), transformed.getTime("time_of_day_millis"));
  }

  @Test
  @SuppressWarnings("ConstantConditions")
  public void testTransformOData4GeospatialPoint() throws Exception {

    Schema schema = Schema.recordOf("schema",
                                    Schema.Field.of("geometry_point", pointSchema("geometry_point")),
                                    Schema.Field.of("geography_point", pointSchema("geography_point")));

    StructuredRecord expectedGeometryPoint = StructuredRecord.builder(pointSchema("point-schema"))
      .set(SapODataConstants.GEOSPATIAL_TYPE_FIELD_NAME, "Point")
      .set(SapODataConstants.GEOSPATIAL_COORDINATES_FIELD_NAME, Arrays.asList(1.0, 1.0))
      .build();

    StructuredRecord expectedGeographyPoint = StructuredRecord.builder(pointSchema("point-schema"))
      .set(SapODataConstants.GEOSPATIAL_TYPE_FIELD_NAME, "Point")
      .set(SapODataConstants.GEOSPATIAL_COORDINATES_FIELD_NAME, Arrays.asList(2.0, 2.0))
      .build();

    ODataEntity entity = ODataEntityBuilder.builder()
      .setGeometryPoint("geometry_point", 1.0, 1.0)
      .setGeographyPoint("geography_point", 2.0, 2.0)
      .build();

    ODataEntryToRecordTransformer transformer = new ODataEntryToRecordTransformer(schema);
    StructuredRecord transformed = transformer.transform(entity);
    StructuredRecord actualGeometryPoint = transformed.get("geometry_point");
    StructuredRecord actualGeographyPoint = transformed.get("geography_point");

    Assert.assertNotNull(actualGeometryPoint);
    Assert.assertNotNull(actualGeographyPoint);
    Assert.assertEquals(expectedGeometryPoint.<String>get(SapODataConstants.GEOSPATIAL_TYPE_FIELD_NAME),
                        actualGeometryPoint.get(SapODataConstants.GEOSPATIAL_TYPE_FIELD_NAME));
    Assert.assertEquals(expectedGeometryPoint.<List>get(SapODataConstants.GEOSPATIAL_COORDINATES_FIELD_NAME),
                        actualGeometryPoint.get(SapODataConstants.GEOSPATIAL_COORDINATES_FIELD_NAME));
    Assert.assertEquals(expectedGeographyPoint.<String>get(SapODataConstants.GEOSPATIAL_TYPE_FIELD_NAME),
                        actualGeographyPoint.get(SapODataConstants.GEOSPATIAL_TYPE_FIELD_NAME));
    Assert.assertEquals(expectedGeographyPoint.<List>get(SapODataConstants.GEOSPATIAL_COORDINATES_FIELD_NAME),
                        actualGeographyPoint.get(SapODataConstants.GEOSPATIAL_COORDINATES_FIELD_NAME));
  }

  @Test
  @SuppressWarnings("ConstantConditions")
  public void testTransformOData4GeospatialLineString() throws Exception {

    Schema schema = Schema.recordOf("schema",
                                    Schema.Field.of("geometry_line_string", lineStringSchema("geometry_line_string")),
                                    Schema.Field.of("geography_line_string", lineStringSchema("geography_line_string"))
    );

    List<List<Double>> geometryCoordinates = Arrays.asList(Arrays.asList(1.0, 0.0), Arrays.asList(2.0, 1.0));
    StructuredRecord expectedGeometryLineString = StructuredRecord.builder(lineStringSchema("line-string-schema"))
      .set(SapODataConstants.GEOSPATIAL_TYPE_FIELD_NAME, "LineString")
      .set(SapODataConstants.GEOSPATIAL_COORDINATES_FIELD_NAME, geometryCoordinates)
      .build();

    List<List<Double>> geographyCoordinates = Arrays.asList(Arrays.asList(3.0, 1.0), Arrays.asList(4.0, 1.0));
    StructuredRecord expectedGeographyLineString = StructuredRecord.builder(lineStringSchema("line-string-schema"))
      .set(SapODataConstants.GEOSPATIAL_TYPE_FIELD_NAME, "LineString")
      .set(SapODataConstants.GEOSPATIAL_COORDINATES_FIELD_NAME, geographyCoordinates)
      .build();

    ODataEntity entity = ODataEntityBuilder.builder()
      .setGeometryLineString("geometry_line_string", geometryCoordinates)
      .setGeographyLineString("geography_line_string", geographyCoordinates)
      .build();

    ODataEntryToRecordTransformer transformer = new ODataEntryToRecordTransformer(schema);
    StructuredRecord transformed = transformer.transform(entity);
    StructuredRecord actualGeometryLineString = transformed.get("geometry_line_string");
    StructuredRecord actualGeographyLineString = transformed.get("geography_line_string");

    Assert.assertNotNull(actualGeometryLineString);
    Assert.assertNotNull(actualGeographyLineString);
    Assert.assertEquals(expectedGeometryLineString.<String>get(SapODataConstants.GEOSPATIAL_TYPE_FIELD_NAME),
                        actualGeometryLineString.get(SapODataConstants.GEOSPATIAL_TYPE_FIELD_NAME));
    Assert.assertEquals(expectedGeometryLineString.<List>get(SapODataConstants.GEOSPATIAL_COORDINATES_FIELD_NAME),
                        actualGeometryLineString.get(SapODataConstants.GEOSPATIAL_COORDINATES_FIELD_NAME));
    Assert.assertEquals(expectedGeographyLineString.<String>get(SapODataConstants.GEOSPATIAL_TYPE_FIELD_NAME),
                        actualGeographyLineString.get(SapODataConstants.GEOSPATIAL_TYPE_FIELD_NAME));
    Assert.assertEquals(expectedGeographyLineString.<List>get(SapODataConstants.GEOSPATIAL_COORDINATES_FIELD_NAME),
                        actualGeographyLineString.get(SapODataConstants.GEOSPATIAL_COORDINATES_FIELD_NAME));
  }

  @Test
  @SuppressWarnings("ConstantConditions")
  public void testTransformOData4GeospatialMultiPoint() throws Exception {

    Schema schema = Schema.recordOf("schema",
                                    Schema.Field.of("geometry_multi_point", multiPointSchema("geometry_multi_point")),
                                    Schema.Field.of("geography_multi_point", multiPointSchema("geography_multi_point"))
    );

    List<List<Double>> geometryCoordinates = Arrays.asList(Arrays.asList(1.0, 0.0), Arrays.asList(2.0, 1.0));
    StructuredRecord expectedGeometryMultiPoint = StructuredRecord.builder(multiPointSchema("multi-point-schema"))
      .set(SapODataConstants.GEOSPATIAL_TYPE_FIELD_NAME, "MultiPoint")
      .set(SapODataConstants.GEOSPATIAL_COORDINATES_FIELD_NAME, geometryCoordinates)
      .build();

    List<List<Double>> geographyCoordinates = Arrays.asList(Arrays.asList(3.0, 1.0), Arrays.asList(4.0, 1.0));
    StructuredRecord expectedGeographyMultiPoint = StructuredRecord.builder(multiPointSchema("multi-point-schema"))
      .set(SapODataConstants.GEOSPATIAL_TYPE_FIELD_NAME, "MultiPoint")
      .set(SapODataConstants.GEOSPATIAL_COORDINATES_FIELD_NAME, geographyCoordinates)
      .build();

    ODataEntity entity = ODataEntityBuilder.builder()
      .setGeometryMultiPoint("geometry_multi_point", geometryCoordinates)
      .setGeographyMultiPoint("geography_multi_point", geographyCoordinates)
      .build();

    ODataEntryToRecordTransformer transformer = new ODataEntryToRecordTransformer(schema);
    StructuredRecord transformed = transformer.transform(entity);
    StructuredRecord actualGeometryMultiPoint = transformed.get("geometry_multi_point");
    StructuredRecord actualGeographyMultiPoint = transformed.get("geography_multi_point");

    Assert.assertNotNull(actualGeometryMultiPoint);
    Assert.assertNotNull(actualGeographyMultiPoint);
    Assert.assertEquals(expectedGeometryMultiPoint.<String>get(SapODataConstants.GEOSPATIAL_TYPE_FIELD_NAME),
                        actualGeometryMultiPoint.get(SapODataConstants.GEOSPATIAL_TYPE_FIELD_NAME));
    Assert.assertEquals(expectedGeometryMultiPoint.<List>get(SapODataConstants.GEOSPATIAL_COORDINATES_FIELD_NAME),
                        actualGeometryMultiPoint.get(SapODataConstants.GEOSPATIAL_COORDINATES_FIELD_NAME));
    Assert.assertEquals(expectedGeographyMultiPoint.<String>get(SapODataConstants.GEOSPATIAL_TYPE_FIELD_NAME),
                        actualGeographyMultiPoint.get(SapODataConstants.GEOSPATIAL_TYPE_FIELD_NAME));
    Assert.assertEquals(expectedGeographyMultiPoint.<List>get(SapODataConstants.GEOSPATIAL_COORDINATES_FIELD_NAME),
                        actualGeographyMultiPoint.get(SapODataConstants.GEOSPATIAL_COORDINATES_FIELD_NAME));
  }

  @Test
  @SuppressWarnings("ConstantConditions")
  public void testTransformOData4GeospatialPolygon() throws Exception {

    Schema schema = Schema.recordOf("schema",
                                    Schema.Field.of("geometry_polygon", polygonSchema("geometry_polygon")),
                                    Schema.Field.of("geography_polygon", polygonSchema("geography_polygon"))
    );

    List<List<List<Double>>> coordinates = Arrays.asList(
      // exterior
      Arrays.asList(Arrays.asList(100.0, 0.0), Arrays.asList(110.0, 0.0),
                    Arrays.asList(110.0, 1.0), Arrays.asList(100.0, 1.0),
                    Arrays.asList(100.0, 0.0)),
      // interior
      Arrays.asList(Arrays.asList(100.2, 0.2), Arrays.asList(100.8, 0.2),
                    Arrays.asList(100.8, 0.8), Arrays.asList(100.2, 0.8),
                    Arrays.asList(100.2, 0.2))
    );
    StructuredRecord expectedGeometryPolygon = StructuredRecord.builder(polygonSchema("polygon-schema"))
      .set(SapODataConstants.GEOSPATIAL_TYPE_FIELD_NAME, "Polygon")
      .set(SapODataConstants.GEOSPATIAL_COORDINATES_FIELD_NAME, coordinates)
      .build();

    StructuredRecord expectedGeographyPolygon = StructuredRecord.builder(polygonSchema("polygon-schema"))
      .set(SapODataConstants.GEOSPATIAL_TYPE_FIELD_NAME, "Polygon")
      .set(SapODataConstants.GEOSPATIAL_COORDINATES_FIELD_NAME, coordinates)
      .build();

    ODataEntity entity = ODataEntityBuilder.builder()
      .setGeometryPolygon("geometry_polygon", coordinates)
      .setGeographyPolygon("geography_polygon", coordinates)
      .build();

    ODataEntryToRecordTransformer transformer = new ODataEntryToRecordTransformer(schema);
    StructuredRecord transformed = transformer.transform(entity);
    StructuredRecord actualGeometryPolygon = transformed.get("geometry_polygon");
    StructuredRecord actualGeographyPolygon = transformed.get("geography_polygon");

    Assert.assertNotNull(actualGeometryPolygon);
    Assert.assertNotNull(actualGeographyPolygon);
    Assert.assertEquals(expectedGeometryPolygon.<String>get(SapODataConstants.GEOSPATIAL_TYPE_FIELD_NAME),
                        actualGeometryPolygon.get(SapODataConstants.GEOSPATIAL_TYPE_FIELD_NAME));
    Assert.assertEquals(expectedGeometryPolygon.<List>get(SapODataConstants.GEOSPATIAL_COORDINATES_FIELD_NAME),
                        actualGeometryPolygon.get(SapODataConstants.GEOSPATIAL_COORDINATES_FIELD_NAME));
    Assert.assertEquals(expectedGeographyPolygon.<String>get(SapODataConstants.GEOSPATIAL_TYPE_FIELD_NAME),
                        actualGeographyPolygon.get(SapODataConstants.GEOSPATIAL_TYPE_FIELD_NAME));
    Assert.assertEquals(expectedGeographyPolygon.<List>get(SapODataConstants.GEOSPATIAL_COORDINATES_FIELD_NAME),
                        actualGeographyPolygon.get(SapODataConstants.GEOSPATIAL_COORDINATES_FIELD_NAME));
  }

  @Test
  @SuppressWarnings("ConstantConditions")
  public void testTransformOData4GeospatialMultiLineString() throws Exception {

    Schema schema = Schema.recordOf("schema",
                                    Schema.Field.of("geometry_multi_ls", multiLineStringSchema("geometry_multi_ls")),
                                    Schema.Field.of("geography_multi_ls", multiLineStringSchema("geography_multi_ls"))
    );

    List<List<List<Double>>> coordinates = Arrays.asList(
      // exterior
      Arrays.asList(Arrays.asList(100.0, 0.0), Arrays.asList(110.0, 0.0),
                    Arrays.asList(110.0, 1.0), Arrays.asList(100.0, 1.0),
                    Arrays.asList(100.0, 0.0)),
      // interior
      Arrays.asList(Arrays.asList(100.2, 0.2), Arrays.asList(100.8, 0.2),
                    Arrays.asList(100.8, 0.8), Arrays.asList(100.2, 0.8),
                    Arrays.asList(100.2, 0.2))
    );
    StructuredRecord expectedGeometryMultiLS = StructuredRecord.builder(multiLineStringSchema("multi-ls-schema"))
      .set(SapODataConstants.GEOSPATIAL_TYPE_FIELD_NAME, "MultiLineString")
      .set(SapODataConstants.GEOSPATIAL_COORDINATES_FIELD_NAME, coordinates)
      .build();

    StructuredRecord expectedGeographyMultiLS = StructuredRecord.builder(multiLineStringSchema("multi-ls-schema"))
      .set(SapODataConstants.GEOSPATIAL_TYPE_FIELD_NAME, "MultiLineString")
      .set(SapODataConstants.GEOSPATIAL_COORDINATES_FIELD_NAME, coordinates)
      .build();

    ODataEntity entity = ODataEntityBuilder.builder()
      .setGeometryMultiLineString("geometry_multi_ls", coordinates)
      .setGeographyMultiLineString("geography_multi_ls", coordinates)
      .build();

    ODataEntryToRecordTransformer transformer = new ODataEntryToRecordTransformer(schema);
    StructuredRecord transformed = transformer.transform(entity);
    StructuredRecord actualGeometryMultiLS = transformed.get("geometry_multi_ls");
    StructuredRecord actualGeographyMultiLS = transformed.get("geography_multi_ls");

    Assert.assertNotNull(actualGeometryMultiLS);
    Assert.assertNotNull(actualGeographyMultiLS);
    Assert.assertEquals(expectedGeometryMultiLS.<String>get(SapODataConstants.GEOSPATIAL_TYPE_FIELD_NAME),
                        actualGeometryMultiLS.get(SapODataConstants.GEOSPATIAL_TYPE_FIELD_NAME));
    Assert.assertEquals(expectedGeometryMultiLS.<List>get(SapODataConstants.GEOSPATIAL_COORDINATES_FIELD_NAME),
                        actualGeometryMultiLS.get(SapODataConstants.GEOSPATIAL_COORDINATES_FIELD_NAME));
    Assert.assertEquals(expectedGeographyMultiLS.<String>get(SapODataConstants.GEOSPATIAL_TYPE_FIELD_NAME),
                        actualGeographyMultiLS.get(SapODataConstants.GEOSPATIAL_TYPE_FIELD_NAME));
    Assert.assertEquals(expectedGeographyMultiLS.<List>get(SapODataConstants.GEOSPATIAL_COORDINATES_FIELD_NAME),
                        actualGeographyMultiLS.get(SapODataConstants.GEOSPATIAL_COORDINATES_FIELD_NAME));
  }

  @Test
  @SuppressWarnings("ConstantConditions")
  public void testTransformOData4GeospatialMultiPolygon() throws Exception {

    Schema schema = Schema.recordOf(
      "schema",
      Schema.Field.of("geometry_multi_polygon", multiPolygonSchema("geometry_multi_polygon")),
      Schema.Field.of("geography_multi_polygon", multiPolygonSchema("geography_multi_polygon")));

    List<List<List<List<Double>>>> coordinates = Arrays.asList(
      // first polygon
      Arrays.asList(
        // exterior
        Arrays.asList(Arrays.asList(100.0, 0.0), Arrays.asList(110.0, 0.0),
                      Arrays.asList(110.0, 1.0), Arrays.asList(100.0, 1.0),
                      Arrays.asList(100.0, 0.0)),
        // interior
        Arrays.asList(Arrays.asList(100.2, 0.2), Arrays.asList(100.8, 0.2),
                      Arrays.asList(100.8, 0.8), Arrays.asList(100.2, 0.8),
                      Arrays.asList(100.2, 0.2))
      ),

      // second polygon
      Arrays.asList(
        // exterior
        Arrays.asList(Arrays.asList(100.0, 0.0), Arrays.asList(110.0, 0.0),
                      Arrays.asList(110.0, 1.0), Arrays.asList(100.0, 1.0),
                      Arrays.asList(100.0, 0.0)),
        // interior
        Arrays.asList(Arrays.asList(100.2, 0.2), Arrays.asList(100.8, 0.2),
                      Arrays.asList(100.8, 0.8), Arrays.asList(100.2, 0.8),
                      Arrays.asList(100.2, 0.2))
      )
    );

    StructuredRecord expectedGeometryMultiPolygon = StructuredRecord.builder(multiPolygonSchema("polygon-schema"))
      .set(SapODataConstants.GEOSPATIAL_TYPE_FIELD_NAME, "MultiPolygon")
      .set(SapODataConstants.GEOSPATIAL_COORDINATES_FIELD_NAME, coordinates)
      .build();

    StructuredRecord expectedGeographyMultiPolygon = StructuredRecord.builder(multiPolygonSchema("polygon-schema"))
      .set(SapODataConstants.GEOSPATIAL_TYPE_FIELD_NAME, "MultiPolygon")
      .set(SapODataConstants.GEOSPATIAL_COORDINATES_FIELD_NAME, coordinates)
      .build();

    ODataEntity entity = ODataEntityBuilder.builder()
      .setGeometryMultiPolygon("geometry_multi_polygon", coordinates)
      .setGeographyMultiPolygon("geography_multi_polygon", coordinates)
      .build();

    ODataEntryToRecordTransformer transformer = new ODataEntryToRecordTransformer(schema);
    StructuredRecord transformed = transformer.transform(entity);
    StructuredRecord actualGeometryMultiPolygon = transformed.get("geometry_multi_polygon");
    StructuredRecord actualGeographyMultiPolygon = transformed.get("geography_multi_polygon");

    Assert.assertNotNull(actualGeometryMultiPolygon);
    Assert.assertNotNull(actualGeographyMultiPolygon);
    Assert.assertEquals(expectedGeometryMultiPolygon.<String>get(SapODataConstants.GEOSPATIAL_TYPE_FIELD_NAME),
                        actualGeometryMultiPolygon.get(SapODataConstants.GEOSPATIAL_TYPE_FIELD_NAME));
    Assert.assertEquals(expectedGeometryMultiPolygon.<List>get(SapODataConstants.GEOSPATIAL_COORDINATES_FIELD_NAME),
                        actualGeometryMultiPolygon.get(SapODataConstants.GEOSPATIAL_COORDINATES_FIELD_NAME));
    Assert.assertEquals(expectedGeographyMultiPolygon.<String>get(SapODataConstants.GEOSPATIAL_TYPE_FIELD_NAME),
                        actualGeographyMultiPolygon.get(SapODataConstants.GEOSPATIAL_TYPE_FIELD_NAME));
    Assert.assertEquals(expectedGeographyMultiPolygon.<List>get(SapODataConstants.GEOSPATIAL_COORDINATES_FIELD_NAME),
                        actualGeographyMultiPolygon.get(SapODataConstants.GEOSPATIAL_COORDINATES_FIELD_NAME));
  }

  @Test
  @SuppressWarnings("ConstantConditions")
  public void testTransformOData4GeometryCollection() throws Exception {

    Schema schema = Schema.recordOf("schema",
                                    Schema.Field.of("geometry_collection", collectionSchema("geometry_collection"))
    );

    GeospatialCollection expectedGeometryCollection = geospatialCollection(Geospatial.Dimension.GEOMETRY);
    ODataEntity entity = ODataEntityBuilder.builder()
      .setGeometryCollection("geometry_collection", expectedGeometryCollection)
      .build();

    ODataEntryToRecordTransformer transformer = new ODataEntryToRecordTransformer(schema);
    StructuredRecord transformed = transformer.transform(entity);
    StructuredRecord collection = transformed.get("geometry_collection");

    Assert.assertNotNull(collection);
    List<StructuredRecord> points = collection.get(SapODataConstants.GEO_COLLECTION_POINTS_FIELD_NAME);
    List<StructuredRecord> lineStrings = collection.get(SapODataConstants.GEO_COLLECTION_LINE_STRINGS_FIELD_NAME);
    List<StructuredRecord> polygons = collection.get(SapODataConstants.GEO_COLLECTION_POLYGONS_FIELD_NAME);
    List<StructuredRecord> multiPoints = collection.get(SapODataConstants.GEO_COLLECTION_MULTI_POINTS_FIELD_NAME);
    List<StructuredRecord> multiLs = collection.get(SapODataConstants.GEO_COLLECTION_MULTI_LINE_STRINGS_FIELD_NAME);
    List<StructuredRecord> multiPolygons = collection.get(SapODataConstants.GEO_COLLECTION_MULTI_POLYGONS_FIELD_NAME);

    Assert.assertNotNull(points);
    Assert.assertNotNull(lineStrings);
    Assert.assertNotNull(polygons);
    Assert.assertNotNull(multiPoints);
    Assert.assertNotNull(multiLs);
    Assert.assertNotNull(multiPolygons);
    Assert.assertEquals(1, points.size());
    Assert.assertEquals(1, lineStrings.size());
    Assert.assertEquals(1, polygons.size());
    Assert.assertEquals(1, multiPoints.size());
    Assert.assertEquals(1, multiLs.size());
    Assert.assertEquals(1, multiPolygons.size());
  }

  @Test
  @SuppressWarnings("ConstantConditions")
  public void testTransformOData4GeographyCollection() throws Exception {

    Schema schema = Schema.recordOf("schema",
                                    Schema.Field.of("geography_collection", collectionSchema("geography_collection"))
    );

    GeospatialCollection expectedGeometryCollection = geospatialCollection(Geospatial.Dimension.GEOGRAPHY);
    ODataEntity entity = ODataEntityBuilder.builder()
      .setGeographyCollection("geography_collection", expectedGeometryCollection)
      .build();

    ODataEntryToRecordTransformer transformer = new ODataEntryToRecordTransformer(schema);
    StructuredRecord transformed = transformer.transform(entity);
    StructuredRecord collection = transformed.get("geography_collection");

    Assert.assertNotNull(collection);
    List<StructuredRecord> points = collection.get(SapODataConstants.GEO_COLLECTION_POINTS_FIELD_NAME);
    List<StructuredRecord> lineStrings = collection.get(SapODataConstants.GEO_COLLECTION_LINE_STRINGS_FIELD_NAME);
    List<StructuredRecord> polygons = collection.get(SapODataConstants.GEO_COLLECTION_POLYGONS_FIELD_NAME);
    List<StructuredRecord> multiPoints = collection.get(SapODataConstants.GEO_COLLECTION_MULTI_POINTS_FIELD_NAME);
    List<StructuredRecord> multiLs = collection.get(SapODataConstants.GEO_COLLECTION_MULTI_LINE_STRINGS_FIELD_NAME);
    List<StructuredRecord> multiPolygons = collection.get(SapODataConstants.GEO_COLLECTION_MULTI_POLYGONS_FIELD_NAME);

    Assert.assertNotNull(points);
    Assert.assertNotNull(lineStrings);
    Assert.assertNotNull(polygons);
    Assert.assertNotNull(multiPoints);
    Assert.assertNotNull(multiLs);
    Assert.assertNotNull(multiPolygons);
    Assert.assertEquals(1, points.size());
    Assert.assertEquals(1, lineStrings.size());
    Assert.assertEquals(1, polygons.size());
    Assert.assertEquals(1, multiPoints.size());
    Assert.assertEquals(1, multiLs.size());
    Assert.assertEquals(1, multiPolygons.size());
  }

  private GeospatialCollection geospatialCollection(Geospatial.Dimension dimension) {
    SRID srid = SRID.valueOf("4326");

    List<Point> lineStringPoints = Arrays.asList(pointOf(dimension, 0.0, 1.0), pointOf(dimension, 0.0, 1.0));
    LineString lineString = new LineString(dimension, srid, lineStringPoints);


    List<Point> interior = Arrays.asList(pointOf(dimension, 100.2, 0.2), pointOf(dimension, 100.8, 0.2),
                                         pointOf(dimension, 100.8, 0.8), pointOf(dimension, 100.2, 0.8),
                                         pointOf(dimension, 100.2, 0.2));

    List<Point> exterior = Arrays.asList(pointOf(dimension, 100.0, 0.0), pointOf(dimension, 110.0, 0.0),
                                         pointOf(dimension, 110.0, 1.0), pointOf(dimension, 100.0, 1.0),
                                         pointOf(dimension, 100.0, 0.0));

    Polygon polygon = new Polygon(dimension, srid, interior, exterior);
    MultiPoint multiPoint = new MultiPoint(dimension, srid, lineStringPoints);
    MultiLineString multiLineString = new MultiLineString(dimension, srid, Collections.singletonList(lineString));
    MultiPolygon multiPolygon = new MultiPolygon(dimension, srid, Collections.singletonList(polygon));

    List<Geospatial> geospatials = new ArrayList<>();
    geospatials.add(pointOf(dimension, 0.0, 1.0));
    geospatials.add(lineString);
    geospatials.add(polygon);
    geospatials.add(multiPoint);
    geospatials.add(multiLineString);
    geospatials.add(multiPolygon);

    return new GeospatialCollection(dimension, srid, geospatials);
  }

  private Point pointOf(Geospatial.Dimension dimension, double x, double y) {
    Point point = new Point(dimension, SRID.valueOf("4326"));
    point.setX(x);
    point.setY(y);

    return point;
  }

  @Test
  @SuppressWarnings("ConstantConditions")
  public void testTransformStreamProperties() throws Exception {
    Schema streamRecordSchema = Schema.recordOf(
      "stream-record",
      Schema.Field.of(SapODataConstants.STREAM_ETAG_FIELD_NAME, Schema.of(Schema.Type.STRING)),
      Schema.Field.of(SapODataConstants.STREAM_CONTENT_TYPE_FIELD_NAME, Schema.of(Schema.Type.STRING)),
      Schema.Field.of(SapODataConstants.STREAM_READ_LINK_FIELD_NAME, Schema.of(Schema.Type.STRING)),
      Schema.Field.of(SapODataConstants.STREAM_EDIT_LINK_FIELD_NAME, Schema.of(Schema.Type.STRING)));

    Schema schema = Schema.recordOf("schema", Schema.Field.of("stream", streamRecordSchema));

    StructuredRecord expectedStream = StructuredRecord.builder(streamRecordSchema)
      .set(SapODataConstants.STREAM_ETAG_FIELD_NAME, "W/\"####\"")
      .set(SapODataConstants.STREAM_CONTENT_TYPE_FIELD_NAME, "image/jpeg")
      .set(SapODataConstants.STREAM_READ_LINK_FIELD_NAME, "http://placehold.it/10x10.jpg?read")
      .set(SapODataConstants.STREAM_EDIT_LINK_FIELD_NAME, "http://placehold.it/10x10.jpg?edit")
      .build();

    // Single 'Edm.Stream' property annotated with both 'mediaReadLink' and 'mediaEditLink' will be represented as two
    // separate instances of Olingo ClientLink.
    // See "Stream PropertyMetadata" Section of the "OData JSON Format Version 4.01" document:
    // https://docs.oasis-open.org/odata/odata-json-format/v4.01/csprd05/odata-json-format-v4.01-csprd05.html
    ClientEntityImpl clientEntity = new ClientEntityImpl(new FullQualifiedName("dummy", "name"));
    URI readUri = new URI("http://placehold.it/10x10.jpg?read");
    ClientLinkType readType = ClientLinkType.fromString(Constants.NS_MEDIA_READ_LINK_REL, "image/jpeg");
    clientEntity.addLink(new ClientLink(readUri, readType, "stream", "W/\"####\""));

    URI editUri = new URI("http://placehold.it/10x10.jpg?edit");
    ClientLinkType editType = ClientLinkType.fromString(Constants.NS_MEDIA_EDIT_LINK_REL, "image/jpeg");
    clientEntity.addLink(new ClientLink(editUri, editType, "stream", "W/\"####\""));

    ODataEntity entity = ODataEntity.valueOf(clientEntity);
    ODataEntryToRecordTransformer transformer = new ODataEntryToRecordTransformer(schema);
    StructuredRecord transformed = transformer.transform(entity);

    StructuredRecord actualStream = transformed.get("stream");
    Assert.assertNotNull(actualStream);
    Assert.assertEquals(expectedStream.<String>get(SapODataConstants.STREAM_ETAG_FIELD_NAME),
                        actualStream.get(SapODataConstants.STREAM_ETAG_FIELD_NAME));
    Assert.assertEquals(expectedStream.<String>get(SapODataConstants.STREAM_CONTENT_TYPE_FIELD_NAME),
                        actualStream.get(SapODataConstants.STREAM_CONTENT_TYPE_FIELD_NAME));
    Assert.assertEquals(expectedStream.<String>get(SapODataConstants.STREAM_READ_LINK_FIELD_NAME),
                        actualStream.get(SapODataConstants.STREAM_READ_LINK_FIELD_NAME));
    Assert.assertEquals(expectedStream.<String>get(SapODataConstants.STREAM_EDIT_LINK_FIELD_NAME),
                        actualStream.get(SapODataConstants.STREAM_EDIT_LINK_FIELD_NAME));
  }
}
