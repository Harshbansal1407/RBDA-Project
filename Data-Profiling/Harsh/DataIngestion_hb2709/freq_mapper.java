import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class freq_mapper extends Mapper<LongWritable, Text, Text, LongWritable> {
    private final static LongWritable one = new LongWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Split the CSV row into fields
        String[] fields = value.toString().split(",");

            // Emit the values of 'Year', 'Month', 'Day', 'Hour', and 'Minute' as keys, and '1' as the count
            context.write(new Text("Yr_" + fields[2]), one);  // 'Year'
            context.write(new Text("Month_" + fields[3]), one);  // 'Month'
            context.write(new Text("Day_" + fields[4]), one);  // 'Day'
            context.write(new Text("HH_" + fields[5]), one);  // 'Hour'
            context.write(new Text("MM_" + fields[6]), one);  // 'Minute'
    }
}
