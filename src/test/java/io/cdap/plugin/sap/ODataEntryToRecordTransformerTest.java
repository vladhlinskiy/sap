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
import org.apache.commons.lang.ArrayUtils;
import org.apache.olingo.odata2.api.ep.entry.ODataEntry;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.math.BigDecimal;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.UUID;

/**
 * {@link ODataEntryToRecordTransformer} test.
 */
public class ODataEntryToRecordTransformerTest {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  @SuppressWarnings("ConstantConditions")
  public void testTransform() {
    Schema schema = Schema.recordOf("schema",
                                    Schema.Field.of("binary", Schema.of(Schema.Type.BYTES)),
                                    Schema.Field.of("boolean", Schema.of(Schema.Type.BOOLEAN)),
                                    Schema.Field.of("byte", Schema.of(Schema.Type.INT)),
                                    Schema.Field.of("datetime", Schema.of(Schema.LogicalType.TIMESTAMP_MICROS)),
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
                                    Schema.Field.of("datetimeoffset", Schema.of(Schema.LogicalType.TIMESTAMP_MICROS)),
                                    Schema.Field.of("null", Schema.nullableOf(Schema.of(Schema.Type.STRING))));

    StructuredRecord expected = StructuredRecord.builder(schema)
      .set("binary", "some bytes".getBytes())
      .set("boolean", true)
      .set("byte", (int) Byte.MAX_VALUE)
      .setTimestamp("datetime", ZonedDateTime.now(ZoneOffset.UTC))
      .setDecimal("decimal", new BigDecimal("12.34"))
      .set("double", Double.MAX_VALUE)
      .set("single", Float.MAX_VALUE)
      .set("guid", UUID.randomUUID().toString())
      .set("int16", (int) Short.MAX_VALUE)
      .set("int32", Integer.MAX_VALUE)
      .set("int64", Long.MAX_VALUE)
      .set("sbyte", (int) Byte.MIN_VALUE)
      .set("string", "Some String")
      .setTime("time", LocalTime.now(ZoneOffset.UTC))
      .setTimestamp("datetimeoffset", ZonedDateTime.now(ZoneOffset.UTC))
      .set("null", null)
      .build();

    ODataEntry entry = ODataEntryBuilder.builder()
      .setBinary("binary", expected.<byte[]>get("binary"))
      .setBoolean("boolean", expected.get("boolean"))
      .setByte("byte", expected.<Number>get("byte").byteValue())
      .setDateTime("datetime", GregorianCalendar.from(expected.getTimestamp("datetime")))
      .setDecimal("decimal", expected.getDecimal("decimal"))
      .setDouble("double", expected.<Double>get("double"))
      .setSingle("single", expected.<Float>get("single"))
      .setGuid("guid", UUID.fromString(expected.get("guid")))
      .setInt16("int16", expected.<Number>get("int16").shortValue())
      .setInt32("int32", expected.<Integer>get("int32"))
      .setInt64("int64", expected.<Long>get("int64"))
      .setSByte("sbyte", expected.<Number>get("sbyte").byteValue())
      .setString("string", expected.get("string"))
      .setTime("time", Time.valueOf(expected.getTime("time")))
      .setDateTimeOffset("datetimeoffset", GregorianCalendar.from(expected.getTimestamp("datetimeoffset")))
      .build();

    ODataEntryToRecordTransformer transformer = new ODataEntryToRecordTransformer(schema);
    StructuredRecord transformed = transformer.transform(entry);

    Assert.assertArrayEquals(expected.<byte[]>get("binary"), transformed.get("binary"));
    Assert.assertEquals(expected.<Boolean>get("boolean"), transformed.get("boolean"));
    Assert.assertEquals(expected.<Byte>get("byte"), transformed.get("byte"));
    Assert.assertEquals(expected.getTimestamp("datetime"), transformed.getTimestamp("datetime"));
    Assert.assertEquals(expected.getDecimal("decimal"), transformed.getDecimal("decimal"));
    Assert.assertEquals(expected.<Double>get("double"), transformed.get("double"), 0.00001);
    Assert.assertEquals(expected.<Float>get("single"), transformed.get("single"), 0.00001);
    Assert.assertEquals(expected.<String>get("guid"), transformed.get("guid"));
    Assert.assertEquals(expected.<Short>get("int16"), transformed.get("int16"));
    Assert.assertEquals(expected.<Integer>get("int32"), transformed.get("int32"));
    Assert.assertEquals(expected.<Long>get("int64"), transformed.get("int64"));
    Assert.assertEquals(expected.<Byte>get("sbyte"), transformed.get("sbyte"));
    Assert.assertEquals(expected.<String>get("string"), transformed.get("string"));
    Assert.assertEquals(expected.getTimestamp("datetimeoffset"), transformed.getTimestamp("datetimeoffset"));
    Assert.assertNull(transformed.get("null"));
  }

  @Test
  @SuppressWarnings("ConstantConditions")
  public void testEdmBinary() {
    Schema schema = Schema.recordOf("schema",
                                    Schema.Field.of("byte[]", Schema.of(Schema.Type.BYTES)),
                                    Schema.Field.of("Byte[]", Schema.of(Schema.Type.BYTES)));

    StructuredRecord expected = StructuredRecord.builder(schema)
      .set("byte[]", "some bytes".getBytes())
      .set("Byte[]", "some bytes".getBytes())
      .build();

    ODataEntry entry = ODataEntryBuilder.builder()
      .setBinary("byte[]", expected.<byte[]>get("byte[]"))
      .setBinary("Byte[]", ArrayUtils.toObject(expected.<byte[]>get("Byte[]")))
      .build();

    ODataEntryToRecordTransformer transformer = new ODataEntryToRecordTransformer(schema);
    StructuredRecord transformed = transformer.transform(entry);

    Assert.assertArrayEquals(expected.<byte[]>get("byte[]"), transformed.get("byte[]"));
    Assert.assertArrayEquals(expected.<byte[]>get("Byte[]"), transformed.get("Byte[]"));
  }

  @Test
  @SuppressWarnings("ConstantConditions")
  public void testEdmByte() {
    Schema schema = Schema.recordOf("schema",
                                    Schema.Field.of("short", Schema.of(Schema.Type.INT)),
                                    Schema.Field.of("byte", Schema.of(Schema.Type.INT)),
                                    Schema.Field.of("int", Schema.of(Schema.Type.INT)),
                                    Schema.Field.of("long", Schema.of(Schema.Type.INT)));

    StructuredRecord expected = StructuredRecord.builder(schema)
      .set("short", (int) Byte.MAX_VALUE)
      .set("byte", (int) Byte.MAX_VALUE)
      .set("int", (int) Byte.MAX_VALUE)
      .set("long", (int) Byte.MAX_VALUE)
      .build();

    ODataEntry entry = ODataEntryBuilder.builder()
      .setByte("short", expected.<Number>get("short").shortValue())
      .setByte("byte", expected.<Number>get("byte").byteValue())
      .setByte("int", expected.<Number>get("int").intValue())
      .setByte("long", expected.<Number>get("long").longValue())
      .build();

    ODataEntryToRecordTransformer transformer = new ODataEntryToRecordTransformer(schema);
    StructuredRecord transformed = transformer.transform(entry);

    Assert.assertEquals(expected.<Integer>get("short"), transformed.get("short"));
    Assert.assertEquals(expected.<Integer>get("byte"), transformed.get("byte"));
    Assert.assertEquals(expected.<Integer>get("int"), transformed.get("int"));
    Assert.assertEquals(expected.<Integer>get("long"), transformed.get("long"));
  }

  @Test
  @SuppressWarnings("ConstantConditions")
  public void testEdmDateTime() {
    Schema schema = Schema.recordOf("schema",
                                    Schema.Field.of("calendar_millis", Schema.of(Schema.LogicalType.TIMESTAMP_MILLIS)),
                                    Schema.Field.of("calendar_micros", Schema.of(Schema.LogicalType.TIMESTAMP_MICROS)),
                                    Schema.Field.of("date_millis", Schema.of(Schema.LogicalType.TIMESTAMP_MILLIS)),
                                    Schema.Field.of("date_micros", Schema.of(Schema.LogicalType.TIMESTAMP_MICROS)),
                                    Schema.Field.of("ts_millis",  Schema.of(Schema.LogicalType.TIMESTAMP_MILLIS)),
                                    Schema.Field.of("ts_micros", Schema.of(Schema.LogicalType.TIMESTAMP_MICROS)),
                                    Schema.Field.of("long_millis", Schema.of(Schema.LogicalType.TIMESTAMP_MILLIS)),
                                    Schema.Field.of("long_micros", Schema.of(Schema.LogicalType.TIMESTAMP_MICROS)));

    StructuredRecord expected = StructuredRecord.builder(schema)
      .setTimestamp("calendar_millis", ZonedDateTime.now(ZoneOffset.UTC))
      .setTimestamp("calendar_micros", ZonedDateTime.now(ZoneOffset.UTC))
      .setTimestamp("date_millis", ZonedDateTime.now(ZoneOffset.UTC))
      .setTimestamp("date_micros", ZonedDateTime.now(ZoneOffset.UTC))
      .setTimestamp("ts_millis", ZonedDateTime.now(ZoneOffset.UTC))
      .setTimestamp("ts_micros", ZonedDateTime.now(ZoneOffset.UTC))
      .setTimestamp("long_millis", ZonedDateTime.now(ZoneOffset.UTC))
      .setTimestamp("long_micros", ZonedDateTime.now(ZoneOffset.UTC))
      .build();

    ODataEntry entry = ODataEntryBuilder.builder()
      .setDateTime("calendar_millis", GregorianCalendar.from(expected.getTimestamp("calendar_millis")))
      .setDateTime("calendar_micros", GregorianCalendar.from(expected.getTimestamp("calendar_micros")))
      .setDateTime("date_millis", Date.from(expected.getTimestamp("date_millis").toInstant()))
      .setDateTime("date_micros", Date.from(expected.getTimestamp("date_micros").toInstant()))
      .setDateTime("ts_millis", Timestamp.from(expected.getTimestamp("ts_millis").toInstant()))
      .setDateTime("ts_micros", Timestamp.from(expected.getTimestamp("ts_micros").toInstant()))
      .setDateTime("long_millis",  expected.getTimestamp("long_millis").toInstant().toEpochMilli())
      .setDateTime("long_micros",  expected.getTimestamp("long_micros").toInstant().toEpochMilli())
      .build();

    ODataEntryToRecordTransformer transformer = new ODataEntryToRecordTransformer(schema);
    StructuredRecord transformed = transformer.transform(entry);

    Assert.assertEquals(expected.getTimestamp("calendar_millis"), transformed.getTimestamp("calendar_millis"));
    Assert.assertEquals(expected.getTimestamp("calendar_micros"), transformed.getTimestamp("calendar_micros"));
    Assert.assertEquals(expected.getTimestamp("date_millis"), transformed.getTimestamp("date_millis"));
    Assert.assertEquals(expected.getTimestamp("date_micros"), transformed.getTimestamp("date_micros"));
    Assert.assertEquals(expected.getTimestamp("ts_millis"), transformed.getTimestamp("ts_millis"));
    Assert.assertEquals(expected.getTimestamp("ts_micros"), transformed.getTimestamp("ts_micros"));
    Assert.assertEquals(expected.getTimestamp("long_millis"), transformed.getTimestamp("long_millis"));
    Assert.assertEquals(expected.getTimestamp("long_micros"), transformed.getTimestamp("long_micros"));
  }

  @Test
  @SuppressWarnings("ConstantConditions")
  public void testEdmDateTimeOffset() {
    Schema schema = Schema.recordOf("schema",
                                    Schema.Field.of("calendar_millis", Schema.of(Schema.LogicalType.TIMESTAMP_MILLIS)),
                                    Schema.Field.of("calendar_micros", Schema.of(Schema.LogicalType.TIMESTAMP_MICROS)),
                                    Schema.Field.of("date_millis", Schema.of(Schema.LogicalType.TIMESTAMP_MILLIS)),
                                    Schema.Field.of("date_micros", Schema.of(Schema.LogicalType.TIMESTAMP_MICROS)),
                                    Schema.Field.of("ts_millis",  Schema.of(Schema.LogicalType.TIMESTAMP_MILLIS)),
                                    Schema.Field.of("ts_micros", Schema.of(Schema.LogicalType.TIMESTAMP_MICROS)),
                                    Schema.Field.of("long_millis", Schema.of(Schema.LogicalType.TIMESTAMP_MILLIS)),
                                    Schema.Field.of("long_micros", Schema.of(Schema.LogicalType.TIMESTAMP_MICROS)));

    StructuredRecord expected = StructuredRecord.builder(schema)
      .setTimestamp("calendar_millis", ZonedDateTime.now(ZoneOffset.UTC))
      .setTimestamp("calendar_micros", ZonedDateTime.now(ZoneOffset.UTC))
      .setTimestamp("date_millis", ZonedDateTime.now(ZoneOffset.UTC))
      .setTimestamp("date_micros", ZonedDateTime.now(ZoneOffset.UTC))
      .setTimestamp("ts_millis", ZonedDateTime.now(ZoneOffset.UTC))
      .setTimestamp("ts_micros", ZonedDateTime.now(ZoneOffset.UTC))
      .setTimestamp("long_millis", ZonedDateTime.now(ZoneOffset.UTC))
      .setTimestamp("long_micros", ZonedDateTime.now(ZoneOffset.UTC))
      .build();

    ODataEntry entry = ODataEntryBuilder.builder()
      .setDateTime("calendar_millis", GregorianCalendar.from(expected.getTimestamp("calendar_millis")))
      .setDateTime("calendar_micros", GregorianCalendar.from(expected.getTimestamp("calendar_micros")))
      .setDateTime("date_millis", Date.from(expected.getTimestamp("date_millis").toInstant()))
      .setDateTime("date_micros", Date.from(expected.getTimestamp("date_micros").toInstant()))
      .setDateTime("ts_millis", Timestamp.from(expected.getTimestamp("ts_millis").toInstant()))
      .setDateTime("ts_micros", Timestamp.from(expected.getTimestamp("ts_micros").toInstant()))
      .setDateTime("long_millis",  expected.getTimestamp("long_millis").toInstant().toEpochMilli())
      .setDateTime("long_micros",  expected.getTimestamp("long_micros").toInstant().toEpochMilli())
      .build();

    ODataEntryToRecordTransformer transformer = new ODataEntryToRecordTransformer(schema);
    StructuredRecord transformed = transformer.transform(entry);

    Assert.assertEquals(expected.getTimestamp("calendar_millis"), transformed.getTimestamp("calendar_millis"));
    Assert.assertEquals(expected.getTimestamp("calendar_micros"), transformed.getTimestamp("calendar_micros"));
    Assert.assertEquals(expected.getTimestamp("date_millis"), transformed.getTimestamp("date_millis"));
    Assert.assertEquals(expected.getTimestamp("date_micros"), transformed.getTimestamp("date_micros"));
    Assert.assertEquals(expected.getTimestamp("ts_millis"), transformed.getTimestamp("ts_millis"));
    Assert.assertEquals(expected.getTimestamp("ts_micros"), transformed.getTimestamp("ts_micros"));
    Assert.assertEquals(expected.getTimestamp("long_millis"), transformed.getTimestamp("long_millis"));
    Assert.assertEquals(expected.getTimestamp("long_micros"), transformed.getTimestamp("long_micros"));
  }


}
