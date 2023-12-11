import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DataCleaning {

  public static Boolean runDataCleaningJob(String inputPath, String outputPath) throws Exception {
    Job job = Job.getInstance();
    job.setJarByClass(DataCleaning.class);
    job.setJobName("Data Cleaning");
  
    FileInputFormat.addInputPath(job, new Path(inputPath));
    FileOutputFormat.setOutputPath(job, new Path(outputPath));
    
    job.setMapperClass(DataCleaningMapper.class);
    job.setReducerClass(DataCleaningReducer.class);
  
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);

    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(Text.class);
  
    job.setNumReduceTasks(1);
  
    job.getConfiguration().set("mapreduce.output.textoutputformat.separator", " ");
    
    // System.exit(job.waitForCompletion(true) ? 0 : 1);
    return job.waitForCompletion(true);
  }

  public static Boolean runDataProfilingCountJob(String inputPath, String outputPath) throws Exception {
    Job job = Job.getInstance();
    job.setJarByClass(DataCleaning.class);
    job.setJobName("Data Profiling Count");
  
    FileInputFormat.addInputPath(job, new Path(inputPath));
    FileOutputFormat.setOutputPath(job, new Path(outputPath));
    
    job.setMapperClass(DataProfilingCountMapper.class);
    job.setReducerClass(DataProfilingCountReducer.class);
  
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
  
    job.setNumReduceTasks(1);
  
    job.getConfiguration().set("mapreduce.output.textoutputformat.separator", " ");
    
    // System.exit(job.waitForCompletion(true) ? 0 : 1);
    return job.waitForCompletion(true);
  }

  public static Boolean runDataProfilingValueJob(String inputPath, String outputPath) throws Exception {
    Job job = Job.getInstance();
    job.setJarByClass(DataCleaning.class);
    job.setJobName("Data Profiling Value");
  
    FileInputFormat.addInputPath(job, new Path(inputPath));
    FileOutputFormat.setOutputPath(job, new Path(outputPath));
    
    job.setMapperClass(DataProfilingValueMapper.class);
    job.setReducerClass(DataProfilingValueReducer.class);
  
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
  
    job.setNumReduceTasks(1);
  
    job.getConfiguration().set("mapreduce.output.textoutputformat.separator", " ");
    
    // System.exit(job.waitForCompletion(true) ? 0 : 1);
    return job.waitForCompletion(true);
  }
  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Usage: DataCleaning <input path> <output path>");
      System.exit(-1);
    }
    
    runDataCleaningJob(args[0], args[1] + "/data_cleaning");
    runDataProfilingCountJob(args[1] + "/data_cleaning/part-r-00000", args[1] + "/data_profiling_count");
    runDataProfilingValueJob(args[1] + "/data_cleaning/part-r-00000", args[1] + "/data_profiling_value");
  }
}
