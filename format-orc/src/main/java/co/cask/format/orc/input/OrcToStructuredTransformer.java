/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.format.orc.input;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.hydrator.common.RecordConverter;
import com.google.common.collect.Lists;
import org.apache.orc.TypeDescription;
import org.apache.orc.mapred.OrcStruct;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Creates ORCStruct records from StructuredRecords
 */
public class OrcToStructuredTransformer extends RecordConverter<OrcStruct, StructuredRecord> {
    private final Map<TypeDescription, Schema> schemaCache = new HashMap<>();

    @Override
    public StructuredRecord transform(OrcStruct record, Schema schema) throws IOException {
        return null;
    }

    public StructuredRecord.Builder transform(OrcStruct record, Schema schema, @Nullable String skipField) throws IOException {
        return null;
    }

    public Schema toSchema(TypeDescription schema) {
        List<Schema.Field> fields = Lists.newArrayList();
        List<String> fieldNames = schema.getFieldNames();
        int index = 0;
        for (TypeDescription fieldSchema : schema.getChildren()) {
            String name = fieldNames.get(index);
            if (!fieldSchema.getCategory().isPrimitive()) {
                throw new IllegalArgumentException(String.format(
                        "Schema contains field '%s' with complex type %s. Only primitive types are supported.",
                        name, fieldSchema));
            }
            fields.add(Schema.Field.of(name, getType(fieldSchema)));
            index++;
        }
        return Schema.recordOf("record", fields);
    }

    private Schema getType(TypeDescription typeDescription) {
        switch (typeDescription.getCategory()) {
            case BOOLEAN:
                return Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN));
            case BYTE:
            case SHORT:
            case INT:
                return Schema.nullableOf(Schema.of(Schema.Type.INT));
            case LONG:
                return Schema.nullableOf(Schema.of(Schema.Type.LONG));
            case FLOAT:
                return Schema.nullableOf(Schema.of(Schema.Type.FLOAT));
            case DOUBLE:
            case DECIMAL:
                return Schema.nullableOf(Schema.of(Schema.Type.DOUBLE));
            case CHAR:
            case DATE:
            case STRING:
            case VARCHAR:
            case TIMESTAMP:
                return Schema.nullableOf(Schema.of(Schema.Type.STRING));
            case BINARY:
                return Schema.nullableOf(Schema.of(Schema.Type.BYTES));
            case MAP:
            case LIST:
            case UNION:
            case STRUCT:
            default:
                throw new IllegalArgumentException(String.format("Schema contains field type %s which is currently not supported",
                        typeDescription.getCategory().name()));
        }
    }

}
