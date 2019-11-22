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

package io.cdap.plugin.marklogic.source;

import com.google.common.base.Strings;
import com.google.gson.stream.JsonReader;
import com.marklogic.mapreduce.ContentType;
import com.marklogic.mapreduce.DatabaseDocument;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.format.StructuredRecordStringConverter;
import io.cdap.cdap.format.io.JsonDecoder;
import io.cdap.cdap.format.io.JsonStructuredRecordDatumReader;
import io.cdap.plugin.marklogic.BaseBatchMarkLogicConfig;
import org.json.JSONArray;
import org.json.JSONObject;
import org.json.XML;

import java.io.IOException;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Scanner;
import javax.annotation.Nullable;

/**
 * Transforms {@link DatabaseDocument} to {@link StructuredRecord}.
 */
public class DatabaseDocumentToRecordTransformer {
  private static final JsonStructuredRecordDatumReader JSON_DATUM_READER = new JsonStructuredRecordDatumReader(true);
  private static final Schema DEFAULT_OUTPUT_SCHEMA = Schema.recordOf(
    "output",
    Schema.Field.of("payload", Schema.of(Schema.Type.BYTES))
  );

  private final Schema schema;
  private final Schema modifiedSchema;
  private final BaseBatchMarkLogicConfig.Format format;
  private final String delimiter;
  private final String fileNameField;
  private final String payloadNameField;

  public DatabaseDocumentToRecordTransformer(Schema schema, BaseBatchMarkLogicConfig.Format format, String delimiter,
                                             String fileNameField, String payloadField) {
    this.schema = schema;
    this.format = format;
    this.delimiter = delimiter;
    this.fileNameField = fileNameField;
    this.payloadNameField = payloadField;
    modifiedSchema = getModifiedSchema(schema, fileNameField);
  }

  /**
   * Transforms given {@link DatabaseDocument} to {@link StructuredRecord}.
   *
   * @param path     Path to file
   * @param document Database Document to be transformed.
   * @return {@link StructuredRecord} that corresponds to the given {@link DatabaseDocument}.
   */
  public List<StructuredRecord> transform(String path, DatabaseDocument document) {
    String fileName = path.contains("/") ? path.substring(path.lastIndexOf('/') + 1) : path;

    if (schema.equals(DEFAULT_OUTPUT_SCHEMA)) {
      return transformDefault(fileName, document.getContentAsByteArray());
    }

    if (isTypeCompatible(document.getContentType(), ContentType.JSON, BaseBatchMarkLogicConfig.Format.JSON)) {
      return transformJson(fileName, convertDatabaseDocumentToString(document));
    }

    if (isTypeCompatible(document.getContentType(), ContentType.XML, BaseBatchMarkLogicConfig.Format.XML)) {
      return transformXml(fileName, convertDatabaseDocumentToString(document));
    }

    if (format == BaseBatchMarkLogicConfig.Format.AUTO ||
      isTypeCompatible(document.getContentType(), ContentType.BINARY, BaseBatchMarkLogicConfig.Format.BLOB)) {
      return transformBinary(fileName, payloadNameField, document);
    }

    if (isTypeCompatible(document.getContentType(), ContentType.TEXT, BaseBatchMarkLogicConfig.Format.DELIMITED)) {
      return transformDelimited(fileName, convertDatabaseDocumentToString(document));
    }

    if (format == BaseBatchMarkLogicConfig.Format.TEXT) {
      return transformText(fileName, payloadNameField, document);
    }

    throw new IllegalStateException(String.format(
      "Type '%s' from config is not compatible with type '%s' from document",
      format,
      document.getContentType()
    ));
  }

  List<StructuredRecord> transformDefault(String fileName, byte[] payload) {
    StructuredRecord.Builder builder = getBuilder(fileName);

    builder.set("payloadNameField", payload);

    return Collections.singletonList(builder.build());
  }

  List<StructuredRecord> transformJson(String fileName, String json) {
    List<StructuredRecord> records = new ArrayList<>();

    try (JsonReader reader = new JsonReader(new StringReader(json))) {
      try {
        reader.beginArray();
        while (reader.hasNext()) {
          records.add(readJson(fileName, reader));
        }
        reader.endArray();
      } catch (IllegalStateException e) {
        if (e.getMessage().startsWith("Expected BEGIN_ARRAY")) {
          records.add(readJson(fileName, reader));
        } else {
          throw new IllegalStateException("Failed to parse document, reason: " + e.getMessage(), e);
        }
      }
    } catch (IOException e) {
      throw new IllegalStateException("Failed to parse document, reason: " + e.getMessage(), e);
    }

    return records;
  }

  List<StructuredRecord> transformXml(String fileName, String xml) {
    JSONObject xmlJSONObj = XML.toJSONObject(xml);
    return readXml(fileName, xmlJSONObj);
  }

  List<StructuredRecord> transformText(String fileName, String payloadField, DatabaseDocument document) {
    StructuredRecord.Builder builder = getBuilder(fileName);

    try {
      builder.set(payloadField, document.getContentAsString());
    } catch (UnsupportedEncodingException e) {
      throw new IllegalStateException(e);
    }
    return Collections.singletonList(builder.build());
  }

  List<StructuredRecord> transformBinary(String fileName, String payloadField, DatabaseDocument document) {
    StructuredRecord.Builder builder = getBuilder(fileName);
    builder.set(payloadField, document.getContentAsByteArray());

    return Collections.singletonList(builder.build());
  }

  List<StructuredRecord> transformDelimited(String fileName, String document) {
    Scanner scanner = new Scanner(document);
    List<StructuredRecord> records = new ArrayList<>();

    while (scanner.hasNextLine()) {
      records.add(readDelimited(fileName, scanner.nextLine()));
    }

    return records;
  }

  private boolean isTypeCompatible(ContentType actualType, ContentType expectedType,
                                   BaseBatchMarkLogicConfig.Format expectedFormatType) {
    return actualType == expectedType && (format == BaseBatchMarkLogicConfig.Format.AUTO
      || format == expectedFormatType);
  }

  private StructuredRecord.Builder getBuilder(String fileName) {
    StructuredRecord.Builder builder = StructuredRecord.builder(schema);

    if (!Strings.isNullOrEmpty(fileNameField)) {
      builder.set(fileNameField, fileName);
    }

    return builder;
  }

  private StructuredRecord readJson(String fileName, JsonReader reader) {
    StructuredRecord.Builder builder = getBuilder(fileName);

    StructuredRecord record;
    try {
      record = JSON_DATUM_READER.read(new JsonDecoder(reader), modifiedSchema);
    } catch (IOException e) {
      throw new IllegalStateException("Failed to parse document, reason: " + e.getMessage(), e);
    }

    for (Schema.Field field : modifiedSchema.getFields()) {
      builder.set(field.getName(), record.get(field.getName()));
    }

    return builder.build();
  }

  private StructuredRecord readDelimited(String fileName, String line) {
    StructuredRecord.Builder builder = getBuilder(fileName);

    StructuredRecord record = StructuredRecordStringConverter.fromDelimitedString(line, delimiter, modifiedSchema);

    for (Schema.Field field : modifiedSchema.getFields()) {
      builder.set(field.getName(), record.get(field.getName()));
    }

    return builder.build();
  }

  private List<StructuredRecord> readXml(String fileName, JSONObject xmlJSONObj) {
    if (xmlJSONObj.names().length() > 1) {
      return transformJson(fileName, xmlJSONObj.toString());
    }

    Iterator<String> keyIterator = xmlJSONObj.keys();
    if (keyIterator.hasNext()) {
      String key = keyIterator.next();
      JSONArray array = xmlJSONObj.optJSONArray(key);

      if (array != null) {
        return transformJson(fileName, array.toString());
      }

      return readXml(fileName, xmlJSONObj.getJSONObject(key));
    }

    return Collections.emptyList();
  }

  private static Schema getModifiedSchema(Schema schema, @Nullable String pathField) {
    // Remove file path field from schema

    if (pathField == null) {
      return schema;
    }
    List<Schema.Field> fieldCopies = new ArrayList<>(schema.getFields().size());
    for (Schema.Field field : schema.getFields()) {
      if (!field.getName().equals(pathField)) {
        fieldCopies.add(field);
      }
    }
    return Schema.recordOf(schema.getRecordName(), fieldCopies);
  }


  private static String convertDatabaseDocumentToString(DatabaseDocument document) {
    try {
      return document.getContentAsString();
    } catch (UnsupportedEncodingException e) {
      throw new IllegalStateException(e);
    }
  }
}
