import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class freq_reducer extends Reducer<Text, LongWritable, Text, LongWritable> {
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        long sum = 0;
        // Sum the counts for each unique value of the column
        for (LongWritable value : values) {
            sum += value.get();
        }
        // Emit the column value and its frequency
        context.write(key, new LongWritable(sum));
    }
}
