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

import org.apache.olingo.odata2.api.edm.EdmLiteralKind;
import org.apache.olingo.odata2.api.edm.EdmSimpleTypeException;
import org.apache.olingo.odata2.api.ep.entry.ODataEntry;
import org.apache.olingo.odata2.core.edm.AbstractSimpleType;
import org.apache.olingo.odata2.core.edm.EdmBinary;
import org.apache.olingo.odata2.core.edm.EdmBoolean;
import org.apache.olingo.odata2.core.edm.EdmByte;
import org.apache.olingo.odata2.core.edm.EdmDateTime;
import org.apache.olingo.odata2.core.edm.EdmDateTimeOffset;
import org.apache.olingo.odata2.core.edm.EdmDecimal;
import org.apache.olingo.odata2.core.edm.EdmDouble;
import org.apache.olingo.odata2.core.edm.EdmGuid;
import org.apache.olingo.odata2.core.edm.EdmInt16;
import org.apache.olingo.odata2.core.edm.EdmInt32;
import org.apache.olingo.odata2.core.edm.EdmInt64;
import org.apache.olingo.odata2.core.edm.EdmSByte;
import org.apache.olingo.odata2.core.edm.EdmSingle;
import org.apache.olingo.odata2.core.edm.EdmString;
import org.apache.olingo.odata2.core.edm.EdmTime;
import org.apache.olingo.odata2.core.ep.entry.ODataEntryImpl;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Provides handy methods to construct a {@link ODataEntry} instance for testing.
 * Some of the EDM types can be represented by multiple Java types. For more information see:
 * <a href="https://olingo.apache.org/javadoc/odata2/org/apache/olingo/odata2/api/edm/EdmSimpleType.html">
 * EdmSimpleType
 * </a>
 */
public final class ODataEntryBuilder {

  private final Map<String, Object> properties;

  private ODataEntryBuilder() {
    this.properties = new HashMap<>();
  }

  public static ODataEntryBuilder builder() {
    return new ODataEntryBuilder();
  }

  protected ODataEntryBuilder set(String name, Object value, AbstractSimpleType type) {
    try {
      // Trigger validation
      type.valueToString(value, EdmLiteralKind.DEFAULT, null);
    } catch (EdmSimpleTypeException e) {
      throw new IllegalArgumentException(e);
    }
    this.properties.put(name, value);
    return this;
  }

  public ODataEntryBuilder setInt16(String name, short value) {
    return set(name, value, EdmInt16.getInstance());
  }

  public ODataEntryBuilder setInt16(String name, byte value) {
    return set(name, value, EdmInt16.getInstance());
  }

  public ODataEntryBuilder setInt16(String name, int value) {
    return set(name, value, EdmInt16.getInstance());
  }

  public ODataEntryBuilder setInt16(String name, long value) {
    return set(name, value, EdmInt16.getInstance());
  }

  public ODataEntryBuilder setInt32(String name, int value) {
    return set(name, value, EdmInt32.getInstance());
  }

  public ODataEntryBuilder setInt32(String name, byte value) {
    return set(name, value, EdmInt32.getInstance());
  }

  public ODataEntryBuilder setInt32(String name, short value) {
    return set(name, value, EdmInt32.getInstance());
  }

  public ODataEntryBuilder setInt32(String name, long value) {
    return set(name, value, EdmInt32.getInstance());
  }

  public ODataEntryBuilder setInt64(String name, long value) {
    return set(name, value, EdmInt64.getInstance());
  }

  public ODataEntryBuilder setInt64(String name, byte value) {
    return set(name, value, EdmInt64.getInstance());
  }

  public ODataEntryBuilder setInt64(String name, short value) {
    return set(name, value, EdmInt64.getInstance());
  }

  public ODataEntryBuilder setInt64(String name, int value) {
    return set(name, value, EdmInt64.getInstance());
  }

  public ODataEntryBuilder setInt64(String name, BigInteger value) {
    return set(name, value, EdmInt64.getInstance());
  }

  public ODataEntryBuilder setBinary(String name, byte[] value) {
    return set(name, value, EdmBinary.getInstance());
  }

  public ODataEntryBuilder setBinary(String name, Byte[] value) {
    return set(name, value, EdmBinary.getInstance());
  }

  public ODataEntryBuilder setBoolean(String name, boolean value) {
    return set(name, value, EdmBoolean.getInstance());
  }

  public ODataEntryBuilder setByte(String name, byte value) {
    return set(name, value, EdmByte.getInstance());
  }

  public ODataEntryBuilder setByte(String name, short value) {
    return set(name, value, EdmByte.getInstance());
  }

  public ODataEntryBuilder setByte(String name, int value) {
    return set(name, value, EdmByte.getInstance());
  }

  public ODataEntryBuilder setByte(String name, long value) {
    return set(name, value, EdmByte.getInstance());
  }

  public ODataEntryBuilder setDateTime(String name, Calendar value) {
    return set(name, value, EdmDateTime.getInstance());
  }

  public ODataEntryBuilder setDateTime(String name, Date value) {
    return set(name, value, EdmDateTime.getInstance());
  }

  public ODataEntryBuilder setDateTime(String name, Timestamp value) {
    return set(name, value, EdmDateTime.getInstance());
  }

  public ODataEntryBuilder setDateTime(String name, long value) {
    return set(name, value, EdmDateTime.getInstance());
  }

  public ODataEntryBuilder setDecimal(String name, BigDecimal value) {
    return set(name, value, EdmDecimal.getInstance());
  }

  public ODataEntryBuilder setDecimal(String name, BigInteger value) {
    return set(name, value, EdmDecimal.getInstance());
  }

  public ODataEntryBuilder setDecimal(String name, double value) {
    return set(name, value, EdmDecimal.getInstance());
  }

  public ODataEntryBuilder setDecimal(String name, float value) {
    return set(name, value, EdmDecimal.getInstance());
  }

  public ODataEntryBuilder setDecimal(String name, byte value) {
    return set(name, value, EdmDecimal.getInstance());
  }

  public ODataEntryBuilder setDecimal(String name, short value) {
    return set(name, value, EdmDecimal.getInstance());
  }

  public ODataEntryBuilder setDecimal(String name, int value) {
    return set(name, value, EdmDecimal.getInstance());
  }

  public ODataEntryBuilder setDecimal(String name, long value) {
    return set(name, value, EdmDecimal.getInstance());
  }

  public ODataEntryBuilder setDouble(String name, double value) {
    return set(name, value, EdmDouble.getInstance());
  }

  public ODataEntryBuilder setDouble(String name, float value) {
    return set(name, value, EdmDouble.getInstance());
  }

  public ODataEntryBuilder setDouble(String name, BigDecimal value) {
    return set(name, value, EdmDouble.getInstance());
  }

  public ODataEntryBuilder setDouble(String name, byte value) {
    return set(name, value, EdmDouble.getInstance());
  }

  public ODataEntryBuilder setDouble(String name, short value) {
    return set(name, value, EdmDouble.getInstance());
  }

  public ODataEntryBuilder setDouble(String name, int value) {
    return set(name, value, EdmDouble.getInstance());
  }

  public ODataEntryBuilder setDouble(String name, long value) {
    return set(name, value, EdmDouble.getInstance());
  }

  public ODataEntryBuilder setSingle(String name, float value) {
    return set(name, value, EdmSingle.getInstance());
  }

  public ODataEntryBuilder setSingle(String name, double value) {
    return set(name, value, EdmSingle.getInstance());
  }

  public ODataEntryBuilder setSingle(String name, BigDecimal value) {
    return set(name, value, EdmSingle.getInstance());
  }

  public ODataEntryBuilder setSingle(String name, byte value) {
    return set(name, value, EdmSingle.getInstance());
  }

  public ODataEntryBuilder setSingle(String name, short value) {
    return set(name, value, EdmSingle.getInstance());
  }

  public ODataEntryBuilder setSingle(String name, int value) {
    return set(name, value, EdmSingle.getInstance());
  }

  public ODataEntryBuilder setSingle(String name, long value) {
    return set(name, value, EdmSingle.getInstance());
  }

  public ODataEntryBuilder setGuid(String name, UUID value) {
    return set(name, value, EdmGuid.getInstance());
  }

  public ODataEntryBuilder setSByte(String name, byte value) {
    return set(name, value, EdmSByte.getInstance());
  }

  public ODataEntryBuilder setSByte(String name, short value) {
    return set(name, value, EdmSByte.getInstance());
  }

  public ODataEntryBuilder setSByte(String name, int value) {
    return set(name, value, EdmSByte.getInstance());
  }

  public ODataEntryBuilder setSByte(String name, long value) {
    return set(name, value, EdmSByte.getInstance());
  }

  public ODataEntryBuilder setString(String name, String value) {
    return set(name, value, EdmString.getInstance());
  }

  public ODataEntryBuilder setTime(String name, Calendar value) {
    return set(name, value, EdmTime.getInstance());
  }

  public ODataEntryBuilder setTime(String name, Date value) {
    return set(name, value, EdmTime.getInstance());
  }

  public ODataEntryBuilder setTime(String name, Timestamp value) {
    return set(name, value, EdmTime.getInstance());
  }

  public ODataEntryBuilder setTime(String name, Time value) {
    return set(name, value, EdmTime.getInstance());
  }

  public ODataEntryBuilder setTime(String name, long value) {
    return set(name, value, EdmTime.getInstance());
  }

  public ODataEntryBuilder setDateTimeOffset(String name, Calendar value) {
    return set(name, value, EdmDateTimeOffset.getInstance());
  }

  public ODataEntryBuilder setDateTimeOffset(String name, Timestamp value) {
    return set(name, value, EdmDateTimeOffset.getInstance());
  }

  public ODataEntryBuilder setDateTimeOffset(String name, Date value) {
    return set(name, value, EdmDateTimeOffset.getInstance());
  }

  public ODataEntryBuilder setDateTimeOffset(String name, long value) {
    return set(name, value, EdmDateTimeOffset.getInstance());
  }

  public ODataEntry build() {
    return new ODataEntryImpl(properties, null, null, null);
  }
}
