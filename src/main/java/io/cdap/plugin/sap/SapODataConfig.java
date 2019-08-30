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

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.validation.InvalidConfigPropertyException;
import io.cdap.plugin.common.Constants;
import io.cdap.plugin.common.IdUtils;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;

/**
 * Defines a {@link PluginConfig} that {@link SapODataSource} can use.
 */
public class SapODataConfig extends PluginConfig {

  private static final Set<Schema.Type> SUPPORTED_SIMPLE_TYPES = ImmutableSet.of(Schema.Type.BOOLEAN, Schema.Type.INT,
                                                                                 Schema.Type.FLOAT, Schema.Type.DOUBLE,
                                                                                 Schema.Type.BYTES, Schema.Type.LONG,
                                                                                 Schema.Type.STRING,
                                                                                 Schema.Type.RECORD); // TODO

  private static final Set<Schema.LogicalType> SUPPORTED_LOGICAL_TYPES = ImmutableSet.of(
    Schema.LogicalType.DECIMAL, Schema.LogicalType.TIMESTAMP_MILLIS, Schema.LogicalType.TIMESTAMP_MICROS,
    Schema.LogicalType.TIME_MILLIS, Schema.LogicalType.TIME_MICROS);

  @Name(Constants.Reference.REFERENCE_NAME)
  @Description(Constants.Reference.REFERENCE_NAME_DESCRIPTION)
  private String referenceName;

  @Name(SapODataConstants.ODATA_SERVICE_URL)
  @Description("Root URL of the SAP OData service.")
  @Macro
  private String url;

  @Name(SapODataConstants.RESOURCE_PATH)
  @Description("Path of the SAP OData entity.")
  @Macro
  private String resourcePath;

  @Name(SapODataConstants.QUERY)
  @Description("OData query options to filter the data.")
  @Macro
  @Nullable
  private String query;

  @Name(SapODataConstants.USERNAME)
  @Description("Username for basic authentication.")
  @Macro
  @Nullable
  private String user;

  @Name(SapODataConstants.PASSWORD)
  @Description("Password for basic authentication.")
  @Macro
  @Nullable
  private String password;

  @Name(SapODataConstants.SCHEMA)
  @Description("Schema of records output by the source.")
  @Nullable
  private String schema;

  @Name(SapODataConstants.INCLUDE_METADATA_ANNOTATIONS)
  @Description("Whether the plugin should read SAP metadata annotations and include them to each CDAP record.")
  private boolean includeMetadataAnnotations;

  public SapODataConfig(String referenceName, String url, String resourcePath, String query, String user,
                        String password, String schema, boolean includeMetadataAnnotations) {
    this.referenceName = referenceName;
    this.url = url;
    this.resourcePath = resourcePath;
    this.query = query;
    this.user = user;
    this.password = password;
    this.schema = schema;
    this.includeMetadataAnnotations = includeMetadataAnnotations;
  }

  public String getReferenceName() {
    return referenceName;
  }

  public String getUrl() {
    return url;
  }

  public String getResourcePath() {
    return resourcePath;
  }

  @Nullable
  public String getQuery() {
    return query;
  }

  /**
   * An OData query can contain '$select' option. The $select option specifies a subset of properties to include in the
   * response body. For example, to get only the name and price of each product, the following query can be used:
   * 'http://localhost/odata/Products?$select=Price,Name'.
   *
   * @return empty list if no '$select' query option specified. List of the property names to include, otherwise(order
   * is preserved).
   */
  public List<String> getSelectProperties() {
    if (Strings.isNullOrEmpty(query) || !query.contains("$select")) {
      return Collections.emptyList();
    }
    // There is no straightforward way to parse $select query options using Olingo V2
    String selectOption = "$select=";
    int start = query.indexOf(selectOption) + selectOption.length();
    int end = query.indexOf("&", start);
    String commaSeparatedPropertyNames = end != -1 ? query.substring(start, end) : query.substring(start);

    return Arrays.asList(commaSeparatedPropertyNames.split(","));
  }

  @Nullable
  public String getUser() {
    return user;
  }

  @Nullable
  public String getPassword() {
    return password;
  }

  @Nullable
  public String getSchema() {
    return schema;
  }

  public boolean isIncludeMetadataAnnotations() {
    return includeMetadataAnnotations;
  }

  @Nullable
  public Schema getParsedSchema() {
    try {
      return schema == null ? null : Schema.parseJson(schema);
    } catch (IOException e) {
      throw new InvalidConfigPropertyException("Invalid schema", e, SapODataConstants.SCHEMA);
    }
  }

  /**
   * Validates {@link SapODataConfig} instance.
   */
  public void validate() {
    if (Strings.isNullOrEmpty(referenceName)) {
      throw new InvalidConfigPropertyException("Reference name must be specified", Constants.Reference.REFERENCE_NAME);
    } else {
      try {
        IdUtils.validateId(referenceName);
      } catch (IllegalArgumentException e) {
        // InvalidConfigPropertyException should be thrown instead of IllegalArgumentException
        throw new InvalidConfigPropertyException("Invalid reference name", e, Constants.Reference.REFERENCE_NAME);
      }
    }
    if (!containsMacro(SapODataConstants.ODATA_SERVICE_URL) && Strings.isNullOrEmpty(url)) {
      throw new InvalidConfigPropertyException("OData Service URL must be specified",
                                               SapODataConstants.ODATA_SERVICE_URL);
    }
    if (!containsMacro(SapODataConstants.RESOURCE_PATH) && Strings.isNullOrEmpty(resourcePath)) {
      throw new InvalidConfigPropertyException("Resource path must be specified", SapODataConstants.RESOURCE_PATH);
    }

    if (!Strings.isNullOrEmpty(schema) && !containsMacro(SapODataConstants.SCHEMA)) {
      Schema parsedSchema = getParsedSchema();
      validateSchema(parsedSchema);
    }
  }

  private void validateSchema(Schema parsedSchema) {
    List<Schema.Field> fields = parsedSchema.getFields();
    if (null == fields || fields.isEmpty()) {
      throw new InvalidConfigPropertyException("Schema should contain fields to map", SapODataConstants.SCHEMA);
    }
    for (Schema.Field field : fields) {
      Schema nonNullableSchema = field.getSchema().isNullable() ?
        field.getSchema().getNonNullable() : field.getSchema();
      Schema.Type type = nonNullableSchema.getType();
      Schema.LogicalType logicalType = nonNullableSchema.getLogicalType();
      if (!SUPPORTED_SIMPLE_TYPES.contains(type) && !SUPPORTED_LOGICAL_TYPES.contains(logicalType)) {
        String supportedTypeNames = Stream.concat(SUPPORTED_SIMPLE_TYPES.stream(), SUPPORTED_LOGICAL_TYPES.stream())
          .map(Enum::name)
          .map(String::toLowerCase)
          .collect(Collectors.joining(", "));
        String actualTypeName = logicalType != null ? logicalType.name().toLowerCase() : type.name().toLowerCase();
        String errorMessage = String.format("Field '%s' is of unsupported type '%s'. Supported types are: %s.",
                                            field.getName(), actualTypeName, supportedTypeNames);
        throw new IllegalArgumentException(errorMessage);
      }
    }
  }

  /**
   * Validate that the provided schema is compatible with the inferred schema. The provided schema is compatible if
   * every field is compatible with the corresponding field in the inferred schema. A field is compatible if it is of
   * the same type or is a nullable version of that type. It is assumed that both schemas are record schemas.
   *
   * @param inferredSchema the inferred schema
   * @param providedSchema the provided schema to check compatibility
   * @throws IllegalArgumentException if the schemas are not type compatible
   */
  public static void validateFieldsMatch(Schema inferredSchema, Schema providedSchema) {
    for (Schema.Field field : providedSchema.getFields()) {
      Schema.Field inferredField = inferredSchema.getField(field.getName());
      if (inferredField == null) {
        throw new IllegalArgumentException(String.format("Field '%s' does not exist.", field.getName()));
      }
      Schema inferredFieldSchema = inferredField.getSchema();
      Schema providedFieldSchema = field.getSchema();

      boolean isInferredFieldNullable = inferredFieldSchema.isNullable();
      boolean isProvidedFieldNullable = providedFieldSchema.isNullable();

      Schema.Type inferredFieldType = isInferredFieldNullable ?
        inferredFieldSchema.getNonNullable().getType() : inferredFieldSchema.getType();
      Schema.Type providedFieldType = isProvidedFieldNullable ?
        providedFieldSchema.getNonNullable().getType() : providedFieldSchema.getType();

      if (inferredFieldType != providedFieldType) {
        throw new IllegalArgumentException(
          String.format("Expected field '%s' to be of type '%s', but it is of type '%s'.",
                        field.getName(), providedFieldType.name(), inferredFieldType.name()));
      }

      if (!isInferredFieldNullable && isProvidedFieldNullable) {
        throw new IllegalArgumentException(String.format("Field '%s' should not be nullable.", field.getName()));
      }
    }
  }
}
