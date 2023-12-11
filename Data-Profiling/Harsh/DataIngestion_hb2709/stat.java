import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class stat {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Vol statistics job");
        job.setJarByClass(stat.class);
        job.setNumReduceTasks(1);
        job.setMapperClass(stat_mapper.class);
        job.setReducerClass(stat_reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        TextInputFormat.addInputPath(job, new Path(args[0]));  // Input path
        TextOutputFormat.setOutputPath(job, new Path(args[1]));  // Output path

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
