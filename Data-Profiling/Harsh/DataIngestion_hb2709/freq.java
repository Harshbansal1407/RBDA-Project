import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class freq {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Multi column frequency job");
        job.setNumReduceTasks(1);
        job.setJarByClass(freq.class);
        job.setMapperClass(freq_mapper.class);
        job.setReducerClass(freq_reducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        TextInputFormat.addInputPath(job, new Path(args[0]));  // Input path
        TextOutputFormat.setOutputPath(job, new Path(args[1]));  // Output path

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
