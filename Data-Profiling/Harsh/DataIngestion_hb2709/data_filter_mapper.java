import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.NullWritable;

import java.io.IOException;
import java.util.Map;
import java.util.HashMap;
import java.util.regex.Pattern;

// import javax.naming.Context;

public class data_filter_mapper extends Mapper<LongWritable, Text, NullWritable, Text> {

    // Define street mapping
    private static final Map<String, String> streetMapping = new HashMap<String, String>() {{
        put("\\bstreet\\b", "st");
        put("\\broad\\b", "rd");
        put("\\bparkway\\b", "pky");
        put("\\bhighway\\b", "hwy");
        put("\\blane\\b", "ln");
        put("\\bFranklin D. Roosevelt\\b", "fdr");
        put("\\bf d r\\b", "fdr");
        put("\\bavenue\\b", "ave");
        put("\\bav\\b", "ave");
        put("\\bboulevard\\b", "blvd");
        put("\\bcourt\\b", "ct");
        put("\\bplace\\b", "pl");
        put("\\bpoint\\b", "pt");
        put("\\bsquare\\b", "sq");
        put("\\bturnpike\\b", "tpke");
        put("\\bentrance\\b", "ent");
        put("\\bexit\\b", "ext");
    }};

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if (key.get() > 0) {  // Skip the header
            // Split the CSV row into fields
            String[] fields = value.toString().split(",");

            // Check if the 'Yr' column is not 2000, 2006, or 2022
            // and 'Vol' is within the specified range (0 <= Vol <= 800)
            if (fields.length > 2 && !fields[2].equals("2000") && !fields[2].equals("2006") && !fields[2].equals("2022")
                    && fields.length > 7 && isVolInRange(fields[7])) {

                // Remove specified columns ('SegmentID', 'WktGeom', 'fromSt', 'toSt', 'Direction')
                StringBuilder modifiedLine = new StringBuilder();
                modifiedLine.append(fields[0]).append(',');  // 'RequestID'
                modifiedLine.append(fields[1]).append(',');  // 'Boro'
                modifiedLine.append(fields[2]).append(',');  // 'Yr'
                modifiedLine.append(fields[3]).append(',');  // 'M'
                modifiedLine.append(fields[4]).append(',');  // 'D'
                modifiedLine.append(fields[5]).append(',');  // 'HH'
                modifiedLine.append(fields[6]).append(',');  // 'MM'
                modifiedLine.append(fields[7]).append(',');  // 'Vol'

                // Modify the 'street' column using the defined functino
                String originalStreet = fields[11].toLowerCase().trim();
                String modifiedStreet = convertStreetName(originalStreet);
                modifiedLine.append(modifiedStreet);         // 'street'

                // Output the modified line with NULL key
                context.write(NullWritable.get(), new Text(modifiedLine.toString()));
            }
        }
    }
    //function to check the range of 'Vol'
    private boolean isVolInRange(String vol) {
        try {
            double volValue = Double.parseDouble(vol);
            return volValue >= 0 && volValue <= 800;
        } catch (NumberFormatException e) {
            // Handle the case where 'Vol' is not a valid number
            return false;
        }
    }
    // function which applies changes in the street names
    private String convertStreetName(String name) {
        for (Map.Entry<String, String> entry : streetMapping.entrySet()) {
            // Check for whole-word match using regex and apply mappings
            name = name.replaceAll(entry.getKey(), entry.getValue());
        }
        // Remove '@' if present
        if (name.contains("@")) {
            name = name.split("@")[0].trim();
        }

        // Remove specific words pertaining to direction
        String[] wordsToRemove = {"\\be/b\\b", "\\bn/b\\b", "\\bs/b\\b", "\\bw/b\\b"};
        for (String word : wordsToRemove) {
            name = name.replaceAll(word, "").trim();
        }
        name = name.replaceAll("\\([^)]*\\)", "").trim();
        
        // Remove 'ent' and 'ext' parts
        String[] partsEnt = name.split("\\bent\\b");
        if (partsEnt.length > 1) {
            name = partsEnt[0].isEmpty() ? partsEnt[1].trim() : partsEnt[0].trim();
        }
    
        String[] partsExt = name.split("\\bext\\b");
        if (partsExt.length > 1) {
            name = partsExt[0].isEmpty() ? partsExt[1].trim() : partsExt[0].trim();
        }
        //removing ["st", "nd", "rd", "th"] from end of numerals
        name = name.replaceAll("(\\d+)(st|nd|rd|th)\\b", "$1").trim();
        return name;
    }
}
