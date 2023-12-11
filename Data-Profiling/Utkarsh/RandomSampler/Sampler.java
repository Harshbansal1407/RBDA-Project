import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Sampler {

	public static void main(String[] args) throws Exception {
		if (args.length != 3) {
			System.err.println("Usage: Sampler <input path> <output path> <filter_percentage>");
			System.exit(-1);
		}
		
		Job job = Job.getInstance();
		
		job.setJarByClass(Sampler.class);
		job.setJobName("Random Sampler");

		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(SamplerMapper.class);
		
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		double sampleRate = Double.parseDouble(args[2]);
        job.getConfiguration().setDouble("filter_percentage", sampleRate > 0 ? sampleRate : 10);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
