import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class stat_mapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

    private final Text outputKey = new Text();
    private final DoubleWritable outputValue = new DoubleWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Assuming 'Vol' is in the 8th column 
        String[] fields = value.toString().split(",");
        if (fields.length > 7) {
            try {
                // Extract 'Vol' and convert to double
                double volValue = Double.parseDouble(fields[7]);

                // Emit 'Vol' as key and its value as the length
                outputKey.set("Vol");
                outputValue.set(volValue);

                context.write(outputKey, outputValue);
            } catch (NumberFormatException ignored) {
                // Ignore if 'Vol' is not a valid double
                // this makes it possible to run this mapper even on the original dataset
                // in which the first row does not contain a numeric record
            }
        }
    }
}
