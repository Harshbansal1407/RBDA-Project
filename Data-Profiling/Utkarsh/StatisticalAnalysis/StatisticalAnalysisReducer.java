import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.MapWritable;

import java.util.Map;
import java.util.TreeMap;
import java.util.HashMap;

public class StatisticalAnalysisReducer extends Reducer<NullWritable, MapWritable, NullWritable, MapWritable> {

	private final String MIN = "min";
	private final String MAX = "max";
	private final String SUM = "sum";
	private final String COUNT = "count";
	private final String VALUES = "values";
	private final String AVG = "average";
	private final String MEDIAN = "median";

	private double min = 9999, max = 0, sum = 0.0, count = 0.0;
	private TreeMap<Double, Long> valueCounts = new TreeMap<>();

	@Override
	public void reduce(NullWritable key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
		for (MapWritable mapWritable : values) {
            updateTempMap(mapWritable);
        }
	}

	private void updateTempMap(MapWritable mapWritable) {
		for (Object key : mapWritable.keySet()) {
            Text mapKey = (Text) key;
			if (mapKey.toString().equals(MIN)) {
				min = Math.min(min, Double.parseDouble(mapWritable.get(mapKey).toString()));
			} else if (mapKey.toString().equals(MAX)) {
				max = Math.max(max, Double.parseDouble(mapWritable.get(mapKey).toString()));
			} else if (mapKey.toString().equals(SUM)) {
				sum = sum + Double.parseDouble(mapWritable.get(mapKey).toString());
			} else if (mapKey.toString().equals(COUNT)) {
				count = count + Double.parseDouble(mapWritable.get(mapKey).toString()); 
			} else if (mapKey.toString().equals(VALUES)) {
				addToTreeMap(mapWritable.get(mapKey).toString(), valueCounts);
			}
		}
	}

	private static void addToTreeMap(String inputString, TreeMap<Double, Long> treeMap) {
        // Remove curly braces and split the string into key-value pairs
        String keyValuePairs = inputString.substring(1, inputString.length() - 1);
        String[] pairs = keyValuePairs.split(",");        

        // Parse and add key-value pairs to the TreeMap
        for (String pair : pairs) {
            String[] entry = pair.split("=");
            double key = Double.parseDouble(entry[0].trim());
            long value = Long.parseLong(entry[1].trim());
            treeMap.put(key, value);
        }
    }

	private double getMedianFromMap() {
		long totalSize = 0L;
		for (Double key : valueCounts.keySet()) {
			totalSize += valueCounts.get(key);
		}
		long index = 0;
		for (Double key : valueCounts.keySet()) {
			index += valueCounts.get(key);
			if (index >= totalSize / 2) {
				return key;
			}
		}
		return -11.11;
	}

	@Override
	public void cleanup(Context context) throws IOException, InterruptedException {
		// Output our ten records to the reducers with a
		// null key
		Map<String, String> tempMap = new HashMap<>();
		tempMap.put(MIN, Double.toString(min));
		tempMap.put(MAX, Double.toString(max));
		tempMap.put(AVG, Double.toString(sum / count));
		tempMap.put(MEDIAN, Double.toString(getMedianFromMap()));

		MapWritable outMap = new MapWritable();
		for (String key : tempMap.keySet()) {
			outMap.put(new Text(key), new Text(tempMap.get(key)));
		}
		context.write(NullWritable.get(), outMap);
	}
}
