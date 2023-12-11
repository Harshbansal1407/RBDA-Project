import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.StringTokenizer;

public class ProjWordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	private int map_index;

	@Override
 	public void setup(Context context) throws IOException, InterruptedException {
 		map_index = Integer.parseInt(context.getConfiguration().get("map_index"));
 	}

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] columnValues = value.toString().split(",(?!-)");
        if (columnValues.length != 13) {
			return;
        }
		
		context.write(new Text(columnValues[map_index]), new IntWritable(1));
	}
}
