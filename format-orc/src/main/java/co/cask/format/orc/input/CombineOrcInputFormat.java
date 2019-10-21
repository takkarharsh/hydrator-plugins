package co.cask.format.orc.input;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.hydrator.format.input.PathTrackingInputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReaderWrapper;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;

import java.io.IOException;

/**
 * @author Harsh Takkar
 */
public class CombineOrcInputFormat extends CombineFileInputFormat<NullWritable, StructuredRecord> {

    /**
     * Creates a RecordReader that delegates to some other RecordReader for each path in the input split.
     */
    @Override
    public RecordReader<NullWritable, StructuredRecord> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException {
        return new CombineFileRecordReader<>((CombineFileSplit) split, context, WrapperReader.class);
    }

    /**
     * A wrapper class that's responsible for delegating to a corresponding RecordReader in
     * {@link PathTrackingInputFormat}. All it does is pick the i'th path in the CombineFileSplit to create a
     * FileSplit and use the delegate RecordReader to read that split.
     */
    public static class WrapperReader extends CombineFileRecordReaderWrapper<NullWritable, StructuredRecord> {

        public WrapperReader(CombineFileSplit split, TaskAttemptContext context,
                             Integer idx) throws IOException, InterruptedException {
            super(new PathTrackingOrcInputFormat(), split, context, idx);
        }
    }
}
