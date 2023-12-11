import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.StringTokenizer;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.text.ParseException;

public class PreProcessorMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

    private final int[] columnsToRead = {1, 2, 3, 4, 11, 12};
    private Text mapText = new Text();

    private final String[] substringToSplit = {" N ", " S ", " E ", " W ", " @ ", " WB ", " NB "};
    private final String[] street_identifiers_1 = {"ST", "STREET", "AVE", "AV", "AVENUE", "ROAD", "RD", "BOULEVARD", "BLVD"};
    private final String[] street_identifiers_2 = {"TURNPIKE", "TPKE", "HIGHWAY", "HWY", "PARKWAY", "PKWY", "EXPRESSWAY", "EXPY", "PLAZA", "PZ"};

    private final Set<String> remove_substring_one_before = new HashSet<>(Arrays.asList("LEVEL"));
    private final Set<String> remove_substring_one_after = new HashSet<>(Arrays.asList("EXIT"));
    private final String[] skip_substrings = {"."};
 
	@Override
 	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if (value.toString().contains(",-101,")) {
			return;
		}
		String[] columnValues = value.toString().split(",(?!-)");
        if (columnValues.length != 13 || columnValues[0].equalsIgnoreCase("ID")) {
			return;
        }

        List<String> columns = new ArrayList<>();

        for (int columnIndex : columnsToRead) {
            columns.add(columnValues[columnIndex]);
        }

        String streetColumn = columns.get(columns.size() - 1);
        columns.remove(columns.size() - 1);
        columns.add(getToAndFromLink(streetColumn));

        String recordedTime = columns.get(3);
        columns.remove(3);
        columns.add(3, modifyTimeColumn(recordedTime));
        
        mapText.set(String.join(",", columns));
        context.write(NullWritable.get(), mapText);
 	}

    private String modifyTimeColumn(String inputTime) {
        SimpleDateFormat inputFormat = new SimpleDateFormat("MM/dd/yyyy hh:mm:ss a");
        SimpleDateFormat outputFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        try {
            Date date = inputFormat.parse(inputTime);
            return outputFormat.format(date);
        } catch (ParseException e) {
            e.printStackTrace();
            return "9999-9-9 9:9:9";
        }
    }

    /**
     * Function to split a column (from_link - to_link) into 2 csv values: from_link,to_link
     */
    private String getToAndFromLink(String streetColumn) {
        String[] parts = streetColumn.split("-");
        if (parts.length <= 1) {
            return getFormattedLinkName(parts[0].toUpperCase()) + ",N/A";
        } else {
            return getFormattedLinkName(parts[0].toUpperCase()) + "," + getFormattedLinkName(parts[1].toUpperCase());
        }
    }

    private String getFormattedLinkName(String linkName) {
        return getFormattedPortion(getFilteredPortion(linkName));
    }

    /**
     * Return the portion of link_name which is to be stored in the DB
     */
    private String getFilteredPortion(String linkName) {
        String foundSubstring = findFirstSubstring(linkName, substringToSplit);
        if (foundSubstring != null) {
            String[] parts = linkName.split(foundSubstring, 2);
            String leftPart = parts[0].trim(), rightPart = parts[1].trim();
            
            if (! rightPart.isEmpty() && leftPart.isEmpty()) {
                return rightPart;
            }
            if (! leftPart.isEmpty() && rightPart.isEmpty()) {
                return leftPart;
            }
            if (findFirstSubstring(rightPart, street_identifiers_1) != null) {
                return rightPart;
            }
            if (findFirstSubstring(leftPart, street_identifiers_1) != null) {
                return leftPart;
            }
            if (findFirstSubstring(rightPart, street_identifiers_2) != null) {
                return rightPart;
            }
            if (findFirstSubstring(leftPart, street_identifiers_2) != null) {
                return leftPart;
            }
            int leftCount = leftPart.split(" ").length, rightCount = rightPart.split(" ").length;
            if (rightCount > leftCount) {
                return rightPart;
            }
            if (leftCount > rightCount) {
                return leftPart;
            }
            if (rightPart.length() >= leftPart.length()) {
                return rightPart;
            }
            return leftPart;
        } else {
            return linkName;
        }
    }

    private String findFirstSubstring(String input, String[] substrings) {
        for (String substring : substrings) {
            if (input.contains(substring)) {
                return substring;
            }
        }
        return null;
    }

    /**
     * Format the fiiltered portion of the link_name for uniformity among different tables
     */
    private String getFormattedPortion(String linkName) {
        linkName = linkName.replace("(", "").replace(")", "");
        Map<String, String> wordMappings = new HashMap<>();
        wordMappings.put("STREET", "ST"); wordMappings.put("AVENUE", "AVE"); wordMappings.put("AV", "AVE"); wordMappings.put("BOULEVARD", "BLVD");
        wordMappings.put("ROAD", "RD"); wordMappings.put("PARKWAY", "PKY"); wordMappings.put("HIGHWAY", "HWY"); wordMappings.put("COURT", "CT");
        wordMappings.put("PLACE", "PL"); wordMappings.put("SQUARE", "SQ"); wordMappings.put("TURNPIKE", "TPKE"); wordMappings.put("LANE", "LN");
        wordMappings.put("POINT", "PT"); wordMappings.put("PLAZA", "PZ");

        String[] words = linkName.split(" ");
        for (int index = 0; index < words.length; index++) {
            if (words[index] == "") continue;
            words[index] = words[index].trim();
            
            words[index] = mapWords(words[index], wordMappings);
            words[index] = removeSuffixesAfterNumeral(words[index]);

            if (remove_substring_one_after.contains(words[index])) {
                words[index] = "";
                if (index+1 < words.length) words[index + 1] = "";
            }
            if (remove_substring_one_before.contains(words[index])) {
                words[index] = "";
                if (index-1 >= 0) words[index - 1] = "";
            }
            if (findFirstSubstring(words[index], skip_substrings) != null) {
                words[index] = "";
            }
        }

        return combineStrings(words);
    }

    private String mapWords(String input, Map<String, String> wordMappings) {
        if (wordMappings.containsKey(input)) {
            return wordMappings.get(input);
        }
        return input;
    }

    private String removeSuffixesAfterNumeral(String input) {
        String regex = "(\\d+)(ST|ND|RD|TH)\\b";        
        return input.replaceAll(regex, "$1");
    }

    private String combineStrings(String[] strings) {
        StringBuilder stringBuilder = new StringBuilder();
        for (String str : strings) {
            if (!str.isEmpty()) {
                if (stringBuilder.length() > 0) {
                    stringBuilder.append(" ");  // Add space between non-empty strings
                }
                stringBuilder.append(str);
            }
        }
        return stringBuilder.toString();
    }
}
