import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.NullWritable;

public class FilterReducer
        extends Reducer<Text, NullWritable, Text, NullWritable> {
    private final NullWritable nullWritable = NullWritable.get();
    @Override
    public void reduce(Text key, Iterable<NullWritable> values, Context context)
            throws IOException, InterruptedException {

        for (NullWritable value : values) {
            context.write(key, nullWritable);
        }
       
   }
}
