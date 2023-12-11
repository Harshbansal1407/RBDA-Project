import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.MapWritable;

import java.util.Map;
import java.util.TreeMap;
import java.util.HashMap;
import java.util.StringTokenizer;

public class StatisticalAnalysisMapper extends Mapper<LongWritable, Text, NullWritable, MapWritable> {

	private int map_index;
	private Map<String, String> tempMap = new HashMap<>();
	private TreeMap<Double, Long> valueCounts = new TreeMap<>();

	private final String MIN = "min";
	private final String MAX = "max";
	private final String SUM = "sum";
	private final String COUNT = "count";
	private final String VALUES = "values";

	@Override
 	public void setup(Context context) throws IOException, InterruptedException {
 		map_index = Integer.parseInt(context.getConfiguration().get("map_index"));
 	}

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		if (value.toString().contains(",-101,")) {
			return;
		}
		String[] columnValues = value.toString().split(",(?!-)");
        if (columnValues.length != 13 || columnValues[0].equalsIgnoreCase("ID")) {
			return;
        }
		
		updateMap(Double.parseDouble(columnValues[map_index]));
	}

	private void updateMap(double val) {
		tempMap.put(SUM, Double.toString(Double.parseDouble(tempMap.getOrDefault(SUM, "0")) + val));
		tempMap.put(COUNT, Double.toString(Double.parseDouble(tempMap.getOrDefault(COUNT, "0")) + 1.0));
		tempMap.put(MIN, Double.toString(Math.min(val, Double.parseDouble(tempMap.getOrDefault(MIN, "9999.0")))));
		tempMap.put(MAX, Double.toString(Math.max(val, Double.parseDouble(tempMap.getOrDefault(MAX, "0.0")))));

		valueCounts.put(val, valueCounts.getOrDefault(val, 0L) + 1L);
	}

	@Override
	public void cleanup(Context context) throws IOException, InterruptedException {
		// Output our ten records to the reducers with a
		// null key
		tempMap.put(VALUES, valueCounts.toString());
		MapWritable outMap = new MapWritable();
		for (String key : tempMap.keySet()) {
			outMap.put(new Text(key), new Text(tempMap.get(key)));
		}
		context.write(NullWritable.get(), outMap);
	}
}
