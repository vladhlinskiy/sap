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
import io.cdap.cdap.etl.api.validation.InvalidConfigPropertyException;
import io.cdap.plugin.common.Constants;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Tests of {@link SapODataConfig} methods.
 */
public class SapODataConfigTest {

  private static final Schema VALID_SCHEMA =
    Schema.recordOf("schema",
                    Schema.Field.of("int_field", Schema.nullableOf(Schema.of(Schema.Type.INT))),
                    Schema.Field.of("long_field", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
                    Schema.Field.of("float_field", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))),
                    Schema.Field.of("double_field", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))),
                    Schema.Field.of("string_field", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                    Schema.Field.of("boolean_field", Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN))),
                    Schema.Field.of("bytes_field", Schema.nullableOf(Schema.of(Schema.Type.BYTES))),
                    Schema.Field.of("decimal_field", Schema.nullableOf(Schema.decimalOf(10, 4))),
                    Schema.Field.of("time_micros_field", Schema.nullableOf(Schema.of(Schema.LogicalType.TIME_MICROS))),
                    Schema.Field.of("time_millis_field", Schema.nullableOf(Schema.of(Schema.LogicalType.TIME_MILLIS))),
                    Schema.Field.of("ts_micros", Schema.nullableOf(Schema.of(Schema.LogicalType.TIMESTAMP_MICROS))),
                    Schema.Field.of("ts_millis", Schema.nullableOf(Schema.of(Schema.LogicalType.TIMESTAMP_MILLIS))));

  private static final SapODataConfig VALID_CONFIG = SapODataConfigBuilder.builder()
    .setReferenceName("SapODataSource")
    .setUrl("http://vhcalnplci.dummy.nodomain:8000/sap/opu/odata/SAP/ZGW100_XX_S2_SRV/")
    .setResourcePath("SalesOrderCollection")
    .setQuery("$top=2&$skip=2&$select=BuyerName&$filter=BuyerName eq %27TECUM%27")
    .setUser("admin")
    .setPassword("password")
    .setSchema(VALID_SCHEMA.toString())
    .build();

  @Test
  public void testValidateValid() {
    VALID_CONFIG.validate();
  }

  @Test
  public void testGetParsedSchema() {
    Assert.assertEquals(VALID_SCHEMA, VALID_CONFIG.getParsedSchema());
  }

  @Test
  public void testValidateReferenceNameNull() {
    try {
      SapODataConfigBuilder.builder(VALID_CONFIG)
        .setReferenceName(null)
        .build()
        .validate();
      Assert.fail("Invalid config should have thrown exception");
    } catch (InvalidConfigPropertyException e) {
      Assert.assertEquals(Constants.Reference.REFERENCE_NAME, e.getProperty());
    }
  }

  @Test
  public void testValidateReferenceNameEmpty() {
    try {
      SapODataConfigBuilder.builder(VALID_CONFIG)
        .setReferenceName("")
        .build()
        .validate();
      Assert.fail("Invalid config should have thrown exception");
    } catch (InvalidConfigPropertyException e) {
      Assert.assertEquals(Constants.Reference.REFERENCE_NAME, e.getProperty());
    }
  }

  @Test
  public void testValidateReferenceNameInvalid() {
    try {
      SapODataConfigBuilder.builder(VALID_CONFIG)
        .setReferenceName("**********")
        .build()
        .validate();
      Assert.fail("Invalid config should have thrown exception");
    } catch (InvalidConfigPropertyException e) {
      Assert.assertEquals(Constants.Reference.REFERENCE_NAME, e.getProperty());
    }
  }

  @Test
  public void testValidateUrlNull() {
    try {
      SapODataConfigBuilder.builder(VALID_CONFIG)
        .setUrl(null)
        .build()
        .validate();
      Assert.fail("Invalid config should have thrown exception");
    } catch (InvalidConfigPropertyException e) {
      Assert.assertEquals(SapODataConstants.ODATA_SERVICE_URL, e.getProperty());
    }
  }

  @Test
  public void testValidateUrlEmpty() {
    try {
      SapODataConfigBuilder.builder(VALID_CONFIG)
        .setUrl("")
        .build()
        .validate();
      Assert.fail("Invalid config should have thrown exception");
    } catch (InvalidConfigPropertyException e) {
      Assert.assertEquals(SapODataConstants.ODATA_SERVICE_URL, e.getProperty());
    }
  }

  @Test
  public void testValidateResourcePathNull() {
    try {
      SapODataConfigBuilder.builder(VALID_CONFIG)
        .setResourcePath(null)
        .build()
        .validate();
      Assert.fail("Invalid config should have thrown exception");
    } catch (InvalidConfigPropertyException e) {
      Assert.assertEquals(SapODataConstants.RESOURCE_PATH, e.getProperty());
    }
  }

  @Test
  public void testValidateResourcePathEmpty() {
    try {
      SapODataConfigBuilder.builder(VALID_CONFIG)
        .setResourcePath("")
        .build()
        .validate();
      Assert.fail("Invalid config should have thrown exception");
    } catch (InvalidConfigPropertyException e) {
      Assert.assertEquals(SapODataConstants.RESOURCE_PATH, e.getProperty());
    }
  }

  @Test
  public void testSelectProperties() {
    List<String> selectProperties = SapODataConfigBuilder.builder(VALID_CONFIG)
      .setQuery("$select=BuyerName")
      .build()
      .getSelectProperties();

    Assert.assertEquals(Collections.singletonList("BuyerName"), selectProperties);
  }

  @Test
  public void testSelectPropertiesMultiple() {
    List<String> selectProperties = SapODataConfigBuilder.builder(VALID_CONFIG)
      .setQuery("$select=Buyer Name,First Name")
      .build()
      .getSelectProperties();

    Assert.assertEquals(Arrays.asList("Buyer Name", "First Name"), selectProperties);
  }

  @Test
  public void testSelectPropertiesAtStart() {
    List<String> selectProperties = SapODataConfigBuilder.builder(VALID_CONFIG)
      .setQuery("$select=BuyerName&$filter=BuyerName eq %27TECUM%27")
      .build()
      .getSelectProperties();

    Assert.assertEquals(Collections.singletonList("BuyerName"), selectProperties);
  }

  @Test
  public void testSelectPropertiesAtEnd() {
    List<String> selectProperties = SapODataConfigBuilder.builder(VALID_CONFIG)
      .setQuery("$top=2&$skip=2&$select=BuyerName")
      .build()
      .getSelectProperties();

    Assert.assertEquals(Collections.singletonList("BuyerName"), selectProperties);
  }

  @Test
  public void testSelectPropertiesAtMiddle() {
    List<String> selectProperties = SapODataConfigBuilder.builder(VALID_CONFIG)
      .setQuery("$top=2&$skip=2&$select=BuyerName&$filter=BuyerName eq %27TECUM%27")
      .build()
      .getSelectProperties();

    Assert.assertEquals(Collections.singletonList("BuyerName"), selectProperties);
  }
}
