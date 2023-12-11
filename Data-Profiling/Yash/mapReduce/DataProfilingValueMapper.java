// package mapReduce;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.IntStream;

import javax.naming.Context;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DataProfilingValueMapper 
    extends Mapper<LongWritable, Text, Text, Text> {

  private static final Integer NUM_COLS = 29;
  
  private static final Integer CRASH_DATE_COL_NUM = 0;
  
  private static final Integer BOROUGH_COL_NUM = 2;

  private static final Integer ZIP_CODE_COL_NUM = 3;

  private static final Integer ON_STREET_NAME_COL_NUM = 6;

  private static final Integer CROSS_STREET_NAME_COL_NUM = 7;

  private static final Integer OFF_STREET_NAME_COL_NUM = 8;

  private static final Integer PERSONS_INJURED_COL_NUM = 9;
  
  private static final Integer PEDESTRIANS_INJURED_COL_NUM = 11;

  private static final Integer CYCLISTS_INJURED_COL_NUM = 13;

  private static final Integer MOTORISTS_INJURED_COL_NUM = 15;

  private static final Integer PERSONS_KILLED_COL_NUM = 10;

  private static final Integer PEDESTRIANS_KILLED_COL_NUM = 12;

  private static final Integer CYCLISTS_KILLED_COL_NUM = 14;

  private static final Integer MOTORISTS_KILLED_COL_NUM = 16;

  private static final int[] COL_INDEX_TO_DROP = { 6 };

  public static String[] parseCSVLine(String csvLine) {
      List<String> fields = new ArrayList<>();
    StringBuilder currentField = new StringBuilder();
    boolean withinQuotes = false;

    for (char c : csvLine.toCharArray()) {
      if (c == ',' && !withinQuotes) {
        fields.add(currentField.toString().trim());
        // fields.add(currentField.toString());
        currentField.setLength(0); // Clear the StringBuilder for the next field
      } else if (c == '\"') {
        withinQuotes = !withinQuotes;
        currentField.append(c);
      } else if (c == '\t') {
        currentField.append(' ');
      } else {
        currentField.append(c);
      }
    }

    // Add the last field
    fields.add(currentField.toString().trim());

    return fields.toArray(new String[0]);
  }

  public static String[] removeFields(String[] fields, int[] indicesToRemove) {
    return IntStream.range(0, fields.length)
        .filter(index -> IntStream.of(indicesToRemove).noneMatch(i -> i == index))
        .mapToObj(i -> fields[i])
        .toArray(String[]::new);
  }

  public static String calculateTotalInjured(String[] fields) {
    Integer totalInjured = 0;
    totalInjured += Integer.parseInt(fields[PERSONS_INJURED_COL_NUM]);
    totalInjured += Integer.parseInt(fields[PEDESTRIANS_INJURED_COL_NUM]);
    totalInjured += Integer.parseInt(fields[CYCLISTS_INJURED_COL_NUM]);
    totalInjured += Integer.parseInt(fields[MOTORISTS_INJURED_COL_NUM]);
    return totalInjured.toString();
  }

  public static String calculateTotalKilled(String[] fields) {
    Integer totalKilled = 0;
    totalKilled += Integer.parseInt(fields[PERSONS_KILLED_COL_NUM]);
    totalKilled += Integer.parseInt(fields[PEDESTRIANS_KILLED_COL_NUM]);
    totalKilled += Integer.parseInt(fields[CYCLISTS_KILLED_COL_NUM]);
    totalKilled += Integer.parseInt(fields[MOTORISTS_KILLED_COL_NUM]);
    return totalKilled.toString();
  }

  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {

    String line = value.toString();
    String[] fields = parseCSVLine(line);

    String crashDate = fields[CRASH_DATE_COL_NUM];
    String[] crashDateParts = crashDate.split("/");
    String month = crashDateParts[0];
    String year = crashDateParts[2];
    String borough = fields[BOROUGH_COL_NUM];

    context.write(new Text("Number of Persons Injured in an accident"), new Text(fields[PERSONS_INJURED_COL_NUM]));
    context.write(new Text("Number of Pedestrians Injured in an accident"), new Text(fields[PEDESTRIANS_INJURED_COL_NUM]));
    context.write(new Text("Number of Cyclists Injured in an accident"), new Text(fields[CYCLISTS_INJURED_COL_NUM]));
    context.write(new Text("Number of Motorists Injured in an accident"), new Text(fields[MOTORISTS_INJURED_COL_NUM]));
    context.write(new Text("Number of Persons Killed in an accident"), new Text(fields[PERSONS_KILLED_COL_NUM]));
    context.write(new Text("Number of Pedestrians Killed in an accident"), new Text(fields[PEDESTRIANS_KILLED_COL_NUM]));
    context.write(new Text("Number of Cyclists Killed in an accident"), new Text(fields[CYCLISTS_KILLED_COL_NUM]));
    context.write(new Text("Number of Motorists Killed in an accident"), new Text(fields[MOTORISTS_KILLED_COL_NUM]));
    context.write(new Text("Total Injured in an accident"), new Text(calculateTotalInjured(fields)));
    context.write(new Text("Total Killed in an accident"), new Text(calculateTotalKilled(fields)));
    context.write(new Text("Total Injured in " + year + " in an accident"), new Text(calculateTotalInjured(fields)));
    context.write(new Text("Total Killed in " + year + " in an accident"), new Text(calculateTotalKilled(fields)));
    context.write(new Text("Total Injured in " + month + " in an accident"), new Text(calculateTotalInjured(fields)));
    context.write(new Text("Total Killed in " + month + " in an accident"), new Text(calculateTotalKilled(fields)));

    if (!borough.equals("")) {
      context.write(new Text("Total Injured in an accident in borough " + borough), new Text(calculateTotalInjured(fields)));
      context.write(new Text("Total Killed in an accident in borough " + borough), new Text(calculateTotalKilled(fields)));
    }
  }
}