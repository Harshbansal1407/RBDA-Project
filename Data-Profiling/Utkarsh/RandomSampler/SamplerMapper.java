import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.StringTokenizer;
import java.util.Random;

public class SamplerMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

	private Random rands = new Random();
 	private Double percentage;
 
	@Override
 	public void setup(Context context) throws IOException, InterruptedException {
 		String strPercentage = context.getConfiguration().get("filter_percentage");
 		percentage = Double.parseDouble(strPercentage) / 100.0;
 	}
 
	@Override
 	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
 		if (rands.nextDouble() < percentage) {
 			context.write(NullWritable.get(), value);
 		}
 	}
}
