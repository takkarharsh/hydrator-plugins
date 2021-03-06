/*
 * Copyright © 2015-2019 Cask Data, Inc.
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

package io.cdap.plugin.transform;

import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.Transform;
import io.cdap.cdap.etl.api.TransformContext;
import io.cdap.plugin.common.KeyValueListParser;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import javax.annotation.Nullable;

/**
 * Projection transform that allows dropping, keeping, renaming, and converting field types.
 */
@Plugin(type = "transform")
@Name("Projection")
@Description("The Projection transform lets you drop, keep, rename, and cast fields to a different type.")
public class ProjectionTransform extends Transform<StructuredRecord, StructuredRecord> {
  private static final String DROP_DESC = "Comma-separated list of fields to drop. For example: " +
    "'field1,field2,field3'. Both drop and keep fields cannot be specified.";
  private static final String RENAME_DESC = "List of fields to rename. This is a comma-separated list of key-value " +
    "pairs, where each pair is separated by a colon and specifies the input and output names. For " +
    "example: 'datestr:date,timestamp:ts' specifies that the 'datestr' field should be renamed to 'date' and the " +
    "'timestamp' field should be renamed to 'ts'.";
  private static final String CONVERT_DESC = "List of fields to convert to a different type. This is a comma-" +
    "separated list of key-value pairs, where each pair is separated by a colon and specifies the field name and " +
    "the desired type. For example: 'count:long,price:double' specifies that the 'count' field should be converted " +
    "to a long and the 'price' field should be converted to a double. Only simple types are supported (boolean, int, " +
    "long, float, double, bytes, string). Any simple type can be converted to bytes or a string. Otherwise, a type " +
    "can only be converted to a larger type. For example, an int can be converted to a long, but a long cannot be " +
    "converted to an int.";
    private static final String KEEP_DESC = "Comma-separated list of fields to keep. For example: " +
     "'field1,field2,field3'. Both keep and drop fields cannot be specified.";

  /**
   * Config class for ProjectionTransform
   */
  public static class ProjectionTransformConfig extends PluginConfig {
    public static final String DROP = "drop";
    public static final String KEEP = "keep";
    public static final String RENAME = "rename";
    public static final String CONVERT = "convert";

    @Description(DROP_DESC)
    @Nullable
    String drop;

    @Description(RENAME_DESC)
    @Nullable
    String rename;

    @Description(CONVERT_DESC)
    @Nullable
    String convert;

    @Description(KEEP_DESC)
    @Nullable
    String keep;
    public ProjectionTransformConfig(String drop, String rename, String convert, String keep) {
      this.drop = drop;
      this.rename = rename;
      this.convert = convert;
      this.keep = keep;
    }
  }

  private final ProjectionTransformConfig projectionTransformConfig;

  public ProjectionTransform(ProjectionTransformConfig projectionTransformConfig) {
    this.projectionTransformConfig = projectionTransformConfig;
  }

  private static final Pattern fieldDelimiter = Pattern.compile("\\s*,\\s*");
  private Set<String> fieldsToDrop = Sets.newHashSet();
  private Set<String> fieldsToKeep = Sets.newHashSet();
  private BiMap<String, String> fieldsToRename = HashBiMap.create();
  private Map<String, Schema.Type> fieldsToConvert = Maps.newHashMap();
  // cache input schema hash to output schema so we don't have to build it each time
  private Map<Schema, Schema> schemaCache = Maps.newHashMap();

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    super.configurePipeline(pipelineConfigurer);
    FailureCollector collector = pipelineConfigurer.getStageConfigurer().getFailureCollector();
    // call init so any invalid config is caught here to fail application creation
    init(pipelineConfigurer.getStageConfigurer().getInputSchema(), collector);
    collector.getOrThrowException();
    Schema outputSchema = null;
    if (pipelineConfigurer.getStageConfigurer().getInputSchema() != null) {
      //validate the input schema and get the output schema for it
      outputSchema = getOutputSchema(pipelineConfigurer.getStageConfigurer().getInputSchema(), collector);
    }
    pipelineConfigurer.getStageConfigurer().setOutputSchema(outputSchema);
  }

  @Override
  public void initialize(TransformContext context) throws Exception {
    super.initialize(context);
    FailureCollector collector = context.getFailureCollector();
    init(null, collector);
    collector.getOrThrowException();
  }

  @Override
  public void transform(StructuredRecord valueIn, Emitter<StructuredRecord> emitter) {
    Schema inputSchema = valueIn.getSchema();
    Schema outputSchema = getOutputSchema(inputSchema, getContext().getFailureCollector());
    StructuredRecord.Builder builder = StructuredRecord.builder(outputSchema);
    for (Schema.Field inputField : inputSchema.getFields()) {
      String inputFieldName = inputField.getName();
      if (!fieldsToKeep.isEmpty() && !fieldsToKeep.contains(inputFieldName)) {
          continue;
      } else if (fieldsToDrop.contains(inputFieldName)) {
        continue;
      }
      // get the corresponding output field name
      String outputFieldName = fieldsToRename.get(inputFieldName);
      if (outputFieldName == null) {
        outputFieldName = inputFieldName;
      }

      Schema.Field outputField = outputSchema.getField(outputFieldName);
      Object inputVal = valueIn.get(inputFieldName);

      // if we need to convert the value, convert it. otherwise just pass the value through
      if (fieldsToConvert.containsKey(inputFieldName)) {
        convertAndSet(builder, outputFieldName, inputVal, inputField.getSchema(), outputField.getSchema());
      } else {
        builder.set(outputFieldName, inputVal);
      }
    }
    emitter.emit(builder.build());
  }

  private void init(Schema inputSchema, FailureCollector collector) {

    if (!Strings.isNullOrEmpty(projectionTransformConfig.drop) &&
      !Strings.isNullOrEmpty(projectionTransformConfig.keep)) {
      collector.addFailure("Cannot specify both drop and keep.", null)
        .withConfigProperty(ProjectionTransformConfig.DROP).withConfigProperty(ProjectionTransformConfig.KEEP);
    }

    if (!Strings.isNullOrEmpty(projectionTransformConfig.drop)) {
      Iterables.addAll(fieldsToDrop, Splitter.on(fieldDelimiter).split(projectionTransformConfig.drop));
      if (inputSchema != null) {
        boolean containAllFields = true;
        for (Schema.Field fieldName : inputSchema.getFields()) {
          if (!fieldsToDrop.contains(fieldName.getName())) {
            containAllFields = false;
          }
        }
        if (containAllFields) {
          collector.addFailure("'Fields to drop' cannot contain all the fields of the input schema.", null)
            .withConfigProperty(ProjectionTransformConfig.DROP);
        }
      }
    } else if (!Strings.isNullOrEmpty(projectionTransformConfig.keep)) {
      Iterables.addAll(fieldsToKeep, Splitter.on(fieldDelimiter).split(projectionTransformConfig.keep));
      if (inputSchema != null) {
        for (String field : fieldsToKeep) {
          if (inputSchema.getField(field) == null) {
            collector.addFailure(
              String.format("Field '%s' provided in 'Fields to keep' must be present in the input schema.",
                            field), null)
              .withConfigElement(ProjectionTransformConfig.KEEP, field);
          }
        }
      }
    }
    KeyValueListParser kvParser = new KeyValueListParser("\\s*,\\s*", ":");
    if (!Strings.isNullOrEmpty(projectionTransformConfig.rename)) {
      for (KeyValue<String, String> keyVal : kvParser.parse(projectionTransformConfig.rename)) {
        String key = keyVal.getKey();
        String val = keyVal.getValue();
        if (inputSchema != null && inputSchema.getField(key) ==  null) {
          collector.addFailure(
            String.format("Field '%s' provided in 'Fields to rename' must be present in the input schema.", key), null)
            .withConfigElement(ProjectionTransformConfig.RENAME, String.format("%s:%s", key, val));
        }
        try {
          String oldVal = fieldsToRename.put(key, val);
          if (oldVal != null) {
            collector.addFailure(
              String.format("Cannot rename '%s' to both '%s' and '%s'.", key, oldVal, val), null)
              .withConfigElement(ProjectionTransformConfig.RENAME, String.format("%s:%s", key, oldVal))
              .withConfigElement(ProjectionTransformConfig.RENAME, String.format("%s:%s", key, val));
          }
        } catch (IllegalArgumentException e) {
          // purely so that we can give a more descriptive error message
          collector.addFailure(
            String.format("Cannot rename more than one field to '%s'.", val), null)
            .withConfigProperty(ProjectionTransformConfig.RENAME);
        }
      }
    }

    if (!Strings.isNullOrEmpty(projectionTransformConfig.convert)) {
      for (KeyValue<String, String> keyVal : kvParser.parse(projectionTransformConfig.convert)) {
        String name = keyVal.getKey();
        String typeStr = keyVal.getValue();
        if (inputSchema != null && inputSchema.getField(name) ==  null) {
          collector.addFailure(
            String.format("Field '%s' provided in 'Convert' is not present in the input schema.", name), null)
            .withConfigElement(ProjectionTransformConfig.CONVERT, String.format("%s:%s", name, typeStr));
        }
        Schema.Type type = Schema.Type.valueOf(typeStr.toUpperCase());
        if (!type.isSimpleType() || type == Schema.Type.NULL) {
          collector.addFailure(
            String.format("Cannot convert field '%s' to a '%s'.", name, typeStr), "Only simple types are supported.")
            .withConfigElement(ProjectionTransformConfig.CONVERT, String.format("%s:%s", name, typeStr));
        }
        if (fieldsToConvert.containsKey(name)) {
          collector.addFailure(
            String.format("Cannot convert '%s' to multiple types.", name), null)
            .withConfigProperty(ProjectionTransformConfig.CONVERT);
        }
        fieldsToConvert.put(name, type);
      }
    }
  }

  private void convertAndSet(StructuredRecord.Builder builder, String fieldName, Object val,
                             Schema inputSchema, Schema outputSchema) {
    // guaranteed that if the input type is nullable, the output type is also nullable.
    Schema.Type inputType = inputSchema.getType();
    Schema.Type outputType = outputSchema.getType();
    if (inputSchema.isNullable()) {
      if (val == null) {
        builder.set(fieldName, null);
        return;
      }
      inputType = inputSchema.getNonNullable().getType();
      outputType = outputSchema.getNonNullable().getType();
    }

    // if the input is a string, try and do some sensible conversion
    if (inputType == Schema.Type.STRING) {
      builder.convertAndSet(fieldName, (String) val);
    } else {
      // otherwise, just try to cast it.
      builder.set(fieldName, convertPrimitive(fieldName, val, inputType, outputType,
                                             getContext().getFailureCollector()));
    }
  }

  private Object convertPrimitive(String fieldName, Object val, Schema.Type inputType,
      Schema.Type outputType, FailureCollector collector) {
    if (inputType == outputType) {
      return val;
    }

    // guaranteed input and output types are non-null simple types
    switch(inputType) {
      // if input is bytes, try to convert the bytes to the correct type
      case BYTES:
        byte[] bytesVal;
        if (val instanceof ByteBuffer) {
          bytesVal = Bytes.toBytes((ByteBuffer) val);
        } else {
          bytesVal = (byte[]) val;
        }
        switch (outputType) {
          case BOOLEAN:
            return Bytes.toBoolean(bytesVal);
          case INT:
            return Bytes.toInt(bytesVal);
          case LONG:
            return Bytes.toLong(bytesVal);
          case FLOAT:
            return Bytes.toFloat(bytesVal);
          case DOUBLE:
            return Bytes.toDouble(bytesVal);
          case STRING:
            return Bytes.toString(bytesVal);
        }
        break;
      case BOOLEAN:
        Boolean boolVal = (Boolean) val;
        switch (outputType) {
          case STRING:
            return String.valueOf(boolVal);
          case BYTES:
            return Bytes.toBytes(boolVal);
        }
        break;
      case INT:
        Integer intVal = (Integer) val;
        switch (outputType) {
          case LONG:
            return intVal.longValue();
          case FLOAT:
            return intVal.floatValue();
          case DOUBLE:
            return intVal.doubleValue();
          case STRING:
            return String.valueOf(intVal);
          case BYTES:
            return Bytes.toBytes(intVal);
        }
        break;
      case LONG:
        Long longVal = (Long) val;
        switch (outputType) {
          case FLOAT:
            return longVal.floatValue();
          case DOUBLE:
            return longVal.doubleValue();
          case STRING:
            return String.valueOf(longVal);
          case BYTES:
            return Bytes.toBytes(longVal);
        }
        break;
      case FLOAT:
        Float floatVal = (Float) val;
        switch (outputType) {
          case DOUBLE:
            return floatVal.doubleValue();
          case STRING:
            return String.valueOf(floatVal);
          case BYTES:
            return Bytes.toBytes(floatVal);
        }
        break;
      case DOUBLE:
        Double doubleVal = (Double) val;
        switch (outputType) {
          case STRING:
            return String.valueOf(doubleVal);
          case BYTES:
            return Bytes.toBytes(doubleVal);
        }
        break;
    }

    String typeStr = outputType.toString().toLowerCase();
    collector.addFailure(
      String.format("Cannot convert field '%s' from type '%s' to type '%s'.", fieldName, inputType, outputType), null)
      .withConfigElement(ProjectionTransformConfig.CONVERT, String.format("%s:%s", fieldName, typeStr));
    throw collector.getOrThrowException();
  }

  private Schema getOutputSchema(Schema inputSchema, FailureCollector collector) {
    Schema output = schemaCache.get(inputSchema);
    if (output != null) {
      return output;
    }

    List<Schema.Field> outputFields = Lists.newArrayList();
    for (Schema.Field inputField : inputSchema.getFields()) {
      String inputFieldName = inputField.getName();
      if (fieldsToDrop.contains(inputFieldName)) {
        continue;
      } else if (!fieldsToKeep.isEmpty() && !fieldsToKeep.contains(inputFieldName)) {
        continue;
      }

      Schema outputFieldSchema = inputField.getSchema();
      // if this is a field that will be converted, figure out the desired schema
      if (fieldsToConvert.containsKey(inputFieldName)) {
        outputFieldSchema = Schema.of(fieldsToConvert.get(inputFieldName));
        Schema inputFieldSchema = inputField.getSchema();
        Schema.Type inputFieldType = inputFieldSchema.getType();

        // if the input was nullable, make the output nullable as well.
        if (inputFieldSchema.isNullable()) {
          inputFieldType = inputFieldSchema.getNonNullable().getType();
          outputFieldSchema = Schema.nullableOf(outputFieldSchema);
        }

        if (!inputFieldType.isSimpleType() || inputFieldType == Schema.Type.NULL) {
          String typeStr = outputFieldSchema.getType().toString().toLowerCase();
          collector.addFailure(
            String.format("Field '%s' is of unconvertible type '%s'.", inputFieldName, inputFieldType), null)
            .withConfigElement(ProjectionTransformConfig.CONVERT, String.format("%s:%s", inputFieldName, typeStr));
          collector.getOrThrowException();
        }
      }

      String outputFieldName = inputFieldName;
      if (fieldsToRename.containsKey(inputFieldName)) {
        outputFieldName = fieldsToRename.get(inputFieldName);
      }

      outputFields.add(Schema.Field.of(outputFieldName, outputFieldSchema));
    }

    output = Schema.recordOf(inputSchema.getRecordName() + ".projected", outputFields);
    schemaCache.put(inputSchema, output);
    return output;
  }
}
