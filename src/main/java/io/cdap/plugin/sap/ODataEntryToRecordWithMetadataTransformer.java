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

import com.google.common.base.Preconditions;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import org.apache.olingo.odata2.api.ep.entry.ODataEntry;

import java.util.Map;

/**
 * Transforms {@link ODataEntry} to {@link StructuredRecord} including metadata annotations.
 */
public class ODataEntryToRecordWithMetadataTransformer extends ODataEntryToRecordTransformer {

  private Map<String, Map<String, String>> fieldNameToMetadata;

  public ODataEntryToRecordWithMetadataTransformer(Schema schema,
                                                   Map<String, Map<String, String>> fieldNameToMetadata) {
    super(schema);
    this.fieldNameToMetadata = fieldNameToMetadata;
  }

  /**
   * Transforms given {@link ODataEntry} to {@link StructuredRecord} including metadata annotations.
   *
   * @param oDataEntry ODataEntry to be transformed.
   * @return {@link StructuredRecord} with metadata annotations that corresponds to the given {@link ODataEntry}.
   */
  public StructuredRecord transform(ODataEntry oDataEntry) {
    StructuredRecord.Builder builder = StructuredRecord.builder(schema);
    for (Schema.Field field : schema.getFields()) {
      Schema nonNullableSchema = field.getSchema().isNullable() ?
        field.getSchema().getNonNullable() : field.getSchema();
      String fieldName = field.getName();
      Object value = oDataEntry.getProperties().get(fieldName);
      builder.set(fieldName, extractValueMetadataRecord(fieldName, value, nonNullableSchema));
    }
    return builder.build();
  }

  private StructuredRecord extractValueMetadataRecord(String fieldName, Object value, Schema schema) {
    Preconditions.checkArgument(schema.getType() == Schema.Type.RECORD); // TODO

    StructuredRecord.Builder builder = StructuredRecord.builder(schema);

    Schema.Field valueField = schema.getField(SapODataConstants.VALUE_FIELD_NAME); // TODO
    Schema valueNonNullableSchema = valueField.getSchema().isNullable()
      ? valueField.getSchema().getNonNullable() : valueField.getSchema();
    builder.set(SapODataConstants.VALUE_FIELD_NAME, extractValue(fieldName, value, valueNonNullableSchema));


    Schema.Field metadataField = schema.getField(SapODataConstants.METADATA_ANNOTATIONS_FIELD_NAME); // TODO
    Schema metadataNonNullableSchema = metadataField.getSchema().isNullable()
      ? metadataField.getSchema().getNonNullable() : metadataField.getSchema();
    StructuredRecord metadataRecord = extractMetadataRecord(fieldName, metadataNonNullableSchema);
    builder.set(SapODataConstants.METADATA_ANNOTATIONS_FIELD_NAME, metadataRecord);

    return builder.build();
  }

  private StructuredRecord extractMetadataRecord(String fieldName, Schema schema) {
    Map<String, String> metadata = fieldNameToMetadata.get(fieldName);
    StructuredRecord.Builder builder = StructuredRecord.builder(schema);
    for (Schema.Field field : schema.getFields()) {
      builder.set(field.getName(), metadata.get(field.getName()));
    }
    return builder.build();
  }
}
