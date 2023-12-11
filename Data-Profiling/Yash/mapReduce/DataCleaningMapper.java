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

public class DataCleaningMapper 
    extends Mapper<LongWritable, Text, Text, Text> {

  private static final Integer NUM_COLS = 29;

  private static final Integer PERSONS_INJURED_COL_NUM = 10;

  private static final Integer PERSONS_KILLED_COL_NUM = 11;

  private static final Integer ON_STREET_NAME_COL_NUM = 7;

  private static final Integer CROSS_STREET_NAME_COL_NUM = 8;

  private static final Integer OFF_STREET_NAME_COL_NUM = 9;

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

  public static String processStreetName(String streetName) {
    if (streetName == null) {
      return null;
    }
    if (streetName.equals("")) {
      return "";
    }

    String processedStreetName = streetName.toLowerCase() + " ";

    processedStreetName = processedStreetName.replace(" avenue ", " ave ");
    processedStreetName = processedStreetName.replace(" av ", " ave ");
    processedStreetName = processedStreetName.replace(" av. ", " ave ");
    processedStreetName = processedStreetName.replace(" ave ", " ave ");
    processedStreetName = processedStreetName.replace(" street ", " st ");
    processedStreetName = processedStreetName.replace(" boulevard ", " blvd "); 
    processedStreetName = processedStreetName.replace(" road ", " rd ");
    processedStreetName = processedStreetName.replace(" parkway ", " pky ");
    processedStreetName = processedStreetName.replace(" highway ", " hwy ");
    processedStreetName = processedStreetName.replace(" court ", " ct ");
    processedStreetName = processedStreetName.replace(" place ", " pl ");
    processedStreetName = processedStreetName.replace(" square ", " sq ");
    processedStreetName = processedStreetName.replace(" turnpike ", " tpke ");
    processedStreetName = processedStreetName.replace(" drive ", " dr ");
    processedStreetName = processedStreetName.replace(" lane ", " ln ");
    processedStreetName = processedStreetName.replace(" plaza ", " plz ");

    return processedStreetName;
  }

  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    
    // Ignore the header line
    if (key.get() == 0) {
      return;
    }

    String line = value.toString();
    String[] fields = parseCSVLine(line);

    // Don't process csv lines with the wrong number of columns
    if (fields.length != NUM_COLS) {
      return;
    }

    // Replace the important columns containing NaN values with 0
    if (fields[PERSONS_INJURED_COL_NUM].equals("")) {
      fields[PERSONS_INJURED_COL_NUM] = "0";
    }
    if (fields[PERSONS_KILLED_COL_NUM].equals("")) {
      fields[PERSONS_KILLED_COL_NUM] = "0";
    }

    // Process the street names according to some rules
    fields[ON_STREET_NAME_COL_NUM] = processStreetName(fields[ON_STREET_NAME_COL_NUM]);
    fields[CROSS_STREET_NAME_COL_NUM] = processStreetName(fields[CROSS_STREET_NAME_COL_NUM]);
    fields[OFF_STREET_NAME_COL_NUM] = processStreetName(fields[OFF_STREET_NAME_COL_NUM]);

    // Remove the columns we don't want
    fields = removeFields(fields, COL_INDEX_TO_DROP);
    
    context.write(new Text(String.valueOf(key)), new Text(String.join(",", fields)));
  }
}