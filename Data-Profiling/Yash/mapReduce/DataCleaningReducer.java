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

public class DataCleaningReducer
    extends Reducer<Text, Text, NullWritable, Text> {

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

    for (Text val : values) {
      context.write(NullWritable.get(), val);
    }
  }
}
