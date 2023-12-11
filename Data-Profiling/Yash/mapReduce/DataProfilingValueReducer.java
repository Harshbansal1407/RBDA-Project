import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import javax.naming.Context;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DataProfilingValueReducer
    extends Reducer<Text, Text, Text, Text> {

  private static final Integer NUM_COLS = 28;

  public static String[] parseCSVLine(String csvLine) {
      List<String> fields = new ArrayList<>();
    StringBuilder currentField = new StringBuilder();
    boolean withinQuotes = false;

    for (char c : csvLine.toCharArray()) {
      if (c == ',' && !withinQuotes) {
        fields.add(currentField.toString().trim());
        currentField.setLength(0); // Clear the StringBuilder for the next field
      } else if (c == '\"') {
        withinQuotes = !withinQuotes;
        currentField.append(c);
      } else {
        currentField.append(c);
      }
    }

    // Add the last field
    fields.add(currentField.toString().trim());

    return fields.toArray(new String[0]);
  }

  @Override
  public void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {

    Integer min = Integer.MAX_VALUE;
    Integer max = Integer.MIN_VALUE;

    for (Text val : values) {
      min = Math.min(min, Integer.parseInt(val.toString()));
      max = Math.max(max, Integer.parseInt(val.toString()));
    }

    context.write(new Text("Min " + key.toString()), new Text(min.toString()));
    context.write(new Text("Max " + key.toString()), new Text(max.toString()));
  }
}
