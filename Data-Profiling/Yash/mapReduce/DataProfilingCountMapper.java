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

public class DataProfilingCountMapper 
    extends Mapper<LongWritable, Text, Text, Text> {

  private static final Integer NUM_COLS = 28;
  
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

    // if (!month.equals("") && !year.equals("")) {
    //   context.write(new Text(month + "/" + year), new Text("1"));
    // }

    // Count the number of missing values in the columns
    if (fields.length != NUM_COLS) {
      context.write(new Text("Number of rows with the wrong number of columns"), new Text("1"));
    }
    if (crashDate.equals("")) {
      context.write(new Text("Number of missing crash date entries"), new Text("1"));
    }
    if (borough.equals("")) {
      context.write(new Text("Number of missing borough entries"), new Text("1"));
    }
    if (fields[ZIP_CODE_COL_NUM].equals("")) {
      context.write(new Text("Number of missing zip code entries"), new Text("1"));
    }
    if (fields[ON_STREET_NAME_COL_NUM].equals("")) {
      context.write(new Text("Number of missing on street name entries"), new Text("1"));
    }
    if (fields[CROSS_STREET_NAME_COL_NUM].equals("")) {
      context.write(new Text("Number of missing cross street name entries"), new Text("1"));
    }
    if (fields[OFF_STREET_NAME_COL_NUM].equals("")) {
      context.write(new Text("Number of missing off street name entries"), new Text("1"));
    }
    if (fields[PERSONS_INJURED_COL_NUM].equals("")) {
      context.write(new Text("Number of missing persons injured entries"), new Text("1"));
    }
    if (fields[PEDESTRIANS_INJURED_COL_NUM].equals("")) {
      context.write(new Text("Number of missing pedestrians injured entries"), new Text("1"));
    }
    if (fields[CYCLISTS_INJURED_COL_NUM].equals("")) {
      context.write(new Text("Number of missing cyclists injured entries"), new Text("1"));
    }
    if (fields[MOTORISTS_INJURED_COL_NUM].equals("")) {
      context.write(new Text("Number of missing motorists injured entries"), new Text("1"));
    }
    if (fields[PERSONS_KILLED_COL_NUM].equals("")) {
      context.write(new Text("Number of missing persons killed entries"), new Text("1"));
    }
    if (fields[PEDESTRIANS_KILLED_COL_NUM].equals("")) {
      context.write(new Text("Number of missing pedestrians killed entries"), new Text("1"));
    }
    if (fields[CYCLISTS_KILLED_COL_NUM].equals("")) {
      context.write(new Text("Number of missing cyclists killed entries"), new Text("1"));
    }
    if (fields[MOTORISTS_KILLED_COL_NUM].equals("")) {
      context.write(new Text("Number of missing motorists killed entries"), new Text("1"));
    }

    // Analyze by year & month
    context.write(new Text("Accidents in " + year), new Text("1"));
    context.write(new Text("Accidents in month " + month), new Text("1"));
    context.write(new Text("People injured in " + year), new Text(calculateTotalInjured(fields)));
    context.write(new Text("People injured in month " + month), new Text(calculateTotalInjured(fields)));
    context.write(new Text("People killed in " + year), new Text(calculateTotalKilled(fields)));
    context.write(new Text("People killed in month " + month), new Text(calculateTotalKilled(fields)));

    // Analyze by borough
    if (!borough.equals("")) {
      context.write(new Text("Accidents in borough " + borough), new Text("1"));
      context.write(new Text("People injured in borough " + borough), new Text(calculateTotalInjured(fields)));
      context.write(new Text("People killed in borough " + borough), new Text(calculateTotalKilled(fields)));
      context.write(new Text("People injured in borough " + borough + " in " + year), new Text(calculateTotalInjured(fields)));
      context.write(new Text("People injured in borough " + borough + " in month " + month), new Text(calculateTotalInjured(fields)));
      context.write(new Text("People killed in borough " + borough + " in " + year), new Text(calculateTotalKilled(fields)));
      context.write(new Text("People killed in borough " + borough + " in month " + month), new Text(calculateTotalKilled(fields)));
      // context.write(new Text("Accidents in borough " + borough + " in " + year), new Text("1"));
      // context.write(new Text("Accidents in borough " + borough + " in month " + month), new Text("1"));
      // context.write(new Text("Accidents in borough " + borough + " in " + year + " in month " + month), new Text("1"));
    }
  }

  @Override
  protected void cleanup(Context context) throws IOException, InterruptedException {
    // Such that these keys are also present in the output, even if they have no values missing
    context.write(new Text("Number of rows with the wrong number of columns"), new Text("0"));
    context.write(new Text("Number of missing crash date entries"), new Text("0"));
    context.write(new Text("Number of missing persons injured entries"), new Text("0"));
    context.write(new Text("Number of missing pedestrians injured entries"), new Text("0"));
    context.write(new Text("Number of missing cyclists injured entries"), new Text("0"));
    context.write(new Text("Number of missing motorists injured entries"), new Text("0"));
    context.write(new Text("Number of missing persons killed entries"), new Text("0"));
    context.write(new Text("Number of missing pedestrians killed entries"), new Text("0"));
    context.write(new Text("Number of missing cyclists killed entries"), new Text("0"));
    context.write(new Text("Number of missing motorists killed entries"), new Text("0"));
  }
}