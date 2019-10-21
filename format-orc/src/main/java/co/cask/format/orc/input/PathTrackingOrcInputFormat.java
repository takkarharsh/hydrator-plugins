package co.cask.format.orc.input;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.hydrator.format.input.PathTrackingInputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.orc.mapred.OrcStruct;
import org.apache.orc.mapreduce.OrcInputFormat;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * @author Harsh Takkar
 */
public class PathTrackingOrcInputFormat extends PathTrackingInputFormat {
    @Override
    protected RecordReader<NullWritable, StructuredRecord.Builder> createRecordReader(FileSplit split,
                                                                                      TaskAttemptContext context,
                                                                                      @Nullable String pathField,
                                                                                      @Nullable Schema schema)
            throws IOException, InterruptedException {
        RecordReader<NullWritable, OrcStruct> delegate = new OrcInputFormat<OrcStruct>()
                .createRecordReader(split, context);
        return new OrcRecordReader(delegate, schema, pathField);
    }

    /**
     * Transforms OrcStruct into StructuredRecord.
     */
    static class OrcRecordReader extends RecordReader<NullWritable, StructuredRecord.Builder> {

        private final RecordReader<NullWritable, OrcStruct> delegate;
        private final OrcToStructuredTransformer recordTransformer;
        private final String pathField;
        private Schema schema;

        public OrcRecordReader(RecordReader<NullWritable, OrcStruct> delegate, @Nullable Schema schema,
                               @Nullable String pathField) {
            this.delegate = delegate;
            this.pathField = pathField;
            this.schema = schema;
            this.recordTransformer = new OrcToStructuredTransformer();
        }

        @Override
        public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
            delegate.initialize(split, context);
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            return delegate.nextKeyValue();
        }

        @Override
        public NullWritable getCurrentKey() {
            return NullWritable.get();
        }

        @Override
        public StructuredRecord.Builder getCurrentValue() throws IOException, InterruptedException {
            //TODO complete the  Implementation
            OrcStruct orcStruct = delegate.getCurrentValue();

            orcStruct.getSchema().getFieldNames();
            orcStruct.getSchema().getChildren().iterator().next();
            if (Objects.isNull(schema)) {
                if (Objects.isNull(pathField)) {
                    recordTransformer.toSchema(orcStruct.getSchema());
                } else {
                    Schema schemaWithoutPath = recordTransformer.toSchema(orcStruct.getSchema());
                    List<Schema.Field> fields = new ArrayList<>(schemaWithoutPath.getFields().size() + 1);
                    fields.addAll(schemaWithoutPath.getFields());
                    fields.add(Schema.Field.of(pathField, Schema.of(Schema.Type.STRING)));
                    schema = Schema.recordOf(schemaWithoutPath.getRecordName(), fields);
                }
            }
            return recordTransformer.transform(orcStruct, schema, pathField);
        }

        @Override
        public float getProgress() throws IOException, InterruptedException {
            return delegate.getProgress();
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }
}
