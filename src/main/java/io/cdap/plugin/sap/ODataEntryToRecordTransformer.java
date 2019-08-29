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
import io.cdap.cdap.api.data.format.UnexpectedFormatException;
import io.cdap.cdap.api.data.schema.Schema;
import org.apache.commons.lang.ArrayUtils;
import org.apache.olingo.odata2.api.ep.entry.ODataEntry;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Calendar;
import java.util.Date;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Transforms {@link ODataEntry} to {@link StructuredRecord}.
 */
public class ODataEntryToRecordTransformer {

  private final Schema schema;

  public ODataEntryToRecordTransformer(Schema schema) {
    this.schema = schema;
  }

  /**
   * Transforms given {@link ODataEntry} to {@link StructuredRecord}.
   *
   * @param oDataEntry ODataEntry to be transformed.
   * @return {@link StructuredRecord} that corresponds to the given {@link ODataEntry}.
   */
  public StructuredRecord transform(ODataEntry oDataEntry) {
    StructuredRecord.Builder builder = StructuredRecord.builder(schema);
    for (Schema.Field field : schema.getFields()) {
      Schema nonNullableSchema = field.getSchema().isNullable() ?
        field.getSchema().getNonNullable() : field.getSchema();
      String fieldName = field.getName();
      Object value = oDataEntry.getProperties().get(fieldName);
      builder.set(fieldName, extractValue(fieldName, value, nonNullableSchema));
    }
    return builder.build();
  }

  /**
   * Extract value of EDM types according to the provided schema. Some of the EDM types can be represented by multiple
   * Java types. For more information see:
   * <a href="https://olingo.apache.org/javadoc/odata2/org/apache/olingo/odata2/api/edm/EdmSimpleType.html">
   * EdmSimpleType
   * </a>
   */
  private Object extractValue(String fieldName, Object value, Schema schema) {
    if (value == null) {
      return null;
    }

    Schema.LogicalType fieldLogicalType = schema.getLogicalType();
    if (fieldLogicalType != null) {
      switch (fieldLogicalType) {
        case TIMESTAMP_MILLIS:
          // Edm.DateTimeOffset, Edm.DateTime
          return extractTimestampMillis(fieldName, value);
        case TIMESTAMP_MICROS:
          // Edm.DateTimeOffset, Edm.DateTime
          return extractTimestampMicros(fieldName, value);
        case TIME_MILLIS:
          // Edm.Time
          return extractTimeMillis(fieldName, value);
        case TIME_MICROS:
          // Edm.Time
          return extractTimeMicros(fieldName, value);
        case DECIMAL:
          return extractDecimal(fieldName, value, schema);
        default:
          throw new UnexpectedFormatException(String.format("Field '%s' is of unsupported type '%s'", fieldName,
                                                            fieldLogicalType.name().toLowerCase()));
      }
    }

    Schema.Type fieldType = schema.getType();
    switch (fieldType) {
      case BOOLEAN:
        // Edm.Boolean
        ensureTypeValid(fieldName, value, Boolean.class);
        return value;
      case INT:
        // Edm.Byte, Edm.Int16, Edm.Int32, Edm.SByte
        ensureTypeValid(fieldName, value, Short.class, Byte.class, Integer.class, Long.class);
        return ((Number) value).intValue();
      case FLOAT:
      case DOUBLE:
        // Edm.Single, Edm.Double
        ensureTypeValid(fieldName, value, Double.class, Float.class, BigDecimal.class, Short.class, Byte.class,
                        Integer.class, Long.class);
        return ((Number) value).doubleValue();
      case BYTES:
        // Edm.Binary
        ensureTypeValid(fieldName, value, byte[].class, Byte[].class);
        if (value instanceof Byte[]) {
          return ArrayUtils.toPrimitive((Byte[]) value);
        }
        return value;
      case LONG:
        // Edm.Int64
        ensureTypeValid(fieldName, value, Short.class, Byte.class, Integer.class, Long.class, BigInteger.class);
        return ((Number) value).longValue();
      case STRING:
        // Edm.String, Edm.Guid
        ensureTypeValid(fieldName, value, String.class, UUID.class);
        return value.toString();
      default:
        throw new UnexpectedFormatException(String.format("Field '%s' is of unsupported type '%s'", fieldName,
                                                          fieldType.name().toLowerCase()));
    }
  }

  private long extractTimeMillis(String fieldName, Object value) {
    ensureTypeValid(fieldName, value, Calendar.class, Date.class, Timestamp.class, Time.class, Long.class);
    if (value instanceof Long) {
      // Edm.Time stored as milliseconds, return as it is
      return (long) value;
    }
    long nanos = extractNanosOfDay(value);
    return TimeUnit.NANOSECONDS.toMillis(nanos);
  }

  private long extractTimeMicros(String fieldName, Object value) {
    ensureTypeValid(fieldName, value, Calendar.class, Date.class, Timestamp.class, Time.class, Long.class);
    if (value instanceof Long) {
      // Edm.Time stored as milliseconds, convert to microseconds first
      return TimeUnit.MILLISECONDS.toMicros((long) value);
    }
    long nanos = extractNanosOfDay(value);
    return TimeUnit.NANOSECONDS.toMicros(nanos);
  }

  private long extractTimestampMillis(String fieldName, Object value) {
    ensureTypeValid(fieldName, value, Calendar.class, Date.class, Timestamp.class, Long.class);
    if (value instanceof Long) {
      // Edm.DateTime, Edm.DateTimeOffset stored as milliseconds, return as it is
      return (long) value;
    }
    Instant instant = extractInstant(value);
    long millis = TimeUnit.SECONDS.toMillis(instant.getEpochSecond());
    return Math.addExact(millis, TimeUnit.NANOSECONDS.toMillis(instant.getNano()));
  }

  private long extractTimestampMicros(String fieldName, Object value) {
    ensureTypeValid(fieldName, value, Calendar.class, Date.class, Timestamp.class, Long.class);
    if (value instanceof Long) {
      // Edm.DateTime, Edm.DateTimeOffset stored as milliseconds, convert to microseconds first
      return TimeUnit.MILLISECONDS.toMicros((long) value);
    }
    Instant instant = extractInstant(value);
    long micros = TimeUnit.SECONDS.toMicros(instant.getEpochSecond());
    return Math.addExact(micros, TimeUnit.NANOSECONDS.toMicros(instant.getNano()));
  }

  private long extractNanosOfDay(Object value) {
    if (value instanceof Calendar) {
      Instant instant = ((Calendar) value).toInstant();
      return LocalDateTime.ofInstant(instant, ZoneOffset.UTC).toLocalTime().toNanoOfDay();
    }
    if (value instanceof Time) {
      return ((Time) value).toLocalTime().toNanoOfDay();
    }
    if (value instanceof Timestamp) {
      return ((Timestamp) value).toLocalDateTime().toLocalTime().toNanoOfDay();
    }
    // instanceof Date
    Instant instant = ((Date) value).toInstant();
    return LocalDateTime.ofInstant(instant, ZoneOffset.UTC).toLocalTime().toNanoOfDay();
  }

  private Instant extractInstant(Object value) {
    if (value instanceof Calendar) {
      return ((Calendar) value).toInstant();
    }
    if (value instanceof Timestamp) {
      return ((Timestamp) value).toInstant();
    }
    // instanceof Date
    return ((Date) value).toInstant();
  }

  private byte[] extractDecimal(String fieldName, Object value, Schema schema) {
    ensureTypeValid(fieldName, value, BigDecimal.class, BigInteger.class, Double.class, Float.class, Byte.class,
                    Short.class, Integer.class, Long.class);

    BigDecimal decimal;
    if (value instanceof BigDecimal) {
      decimal = (BigDecimal) value;
    } else if (value instanceof BigInteger) {
      decimal = new BigDecimal((BigInteger) value);
    } else if ((value instanceof Double) || (value instanceof Float)) {
      double doubleValue = ((Number) value).doubleValue();
      decimal = new BigDecimal(doubleValue);
    } else {
      long longValue = ((Number) value).longValue();
      decimal = new BigDecimal(longValue);
    }

    int schemaPrecision = schema.getPrecision();
    int schemaScale = schema.getScale();
    if (decimal.precision() > schemaPrecision) {
      throw new UnexpectedFormatException(
        String.format("Field '%s' has precision '%s' which is higher than schema precision '%s'.",
                      fieldName, decimal.precision(), schemaPrecision));
    }

    if (decimal.scale() > schemaScale) {
      throw new UnexpectedFormatException(
        String.format("Field '%s' has scale '%s' which is not equal to schema scale '%s'.",
                      fieldName, decimal.scale(), schemaScale));
    }

    return decimal.setScale(schemaScale).unscaledValue().toByteArray();
  }

  private void ensureTypeValid(String fieldName, Object value, Class... expectedTypes) {
    for (Class expectedType : expectedTypes) {
      if (expectedType.isInstance(value)) {
        return;
      }
    }

    String expectedTypeNames = Stream.of(expectedTypes)
      .map(Class::getName)
      .collect(Collectors.joining(", "));
    throw new UnexpectedFormatException(
      String.format("Document field '%s' is expected to be of type '%s', but found a '%s'.", fieldName,
                    expectedTypeNames, value.getClass().getSimpleName()));
  }
}
