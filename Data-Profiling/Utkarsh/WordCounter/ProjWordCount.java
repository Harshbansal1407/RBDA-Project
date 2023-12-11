import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ProjWordCount {

	public static void main(String[] args) throws Exception {
		if (args.length != 3) {
			System.err.println("Usage: WordCount <input path> <output path> <index>");
			System.exit(-1);
		}
		Job job = Job.getInstance();
		job.setJarByClass(ProjWordCount.class);
		job.setJobName("Word Count Job");
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(ProjWordCountMapper.class);
		job.setReducerClass(ProjWordCountReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setNumReduceTasks(1);

		int index = Integer.parseInt(args[2]);
        job.getConfiguration().setInt("map_index", index);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
