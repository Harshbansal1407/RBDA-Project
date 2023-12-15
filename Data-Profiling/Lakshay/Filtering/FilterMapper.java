import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.io.NullWritable;
import java.util.Scanner;

import javax.swing.text.DefaultStyledDocument.ElementSpec;  

import java.util.ArrayList;
import java.util.List;
import java.util.Arrays;
import java.util.Set;
import java.util.HashSet;


public class FilterMapper
    extends Mapper<LongWritable, Text, Text, NullWritable> {
    private static final int MISSING = 9999;

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();

        ArrayList<String> arrayList = new ArrayList<>();
        
        String[] words = customSplit(line, ",");
        
        int length = words.length;
        
        
        if(length < 43)
                // less than 43 columns
            return;
        

        if(words[4].length() < 10)
                //issuing date too short
            return;
        
        if(isValidStateCode(getFirstTwoCharacters(words[2]))){
            words[2] = getFirstTwoCharacters(words[2]);
        }
        else{
            return;
        }
        

        words[2] = getFirstTwoCharacters(words[2]);
        arrayList.add(words[2]);
        words[4] = words[4].substring(0, 10);
        arrayList.add(words[4]);

        words[5] = parseStringToInteger(words[5], 100);
        arrayList.add(words[5]);
        
        if(isValidDate(words[12])  && words[12].length() >= 6){
            words[12] = words[12].substring(0, 6);
        }
        
        words[14] = parseStringToInteger(words[14], 123);
        words[15] = parseStringToInteger(words[15], 123);

        arrayList.add(words[14]);
        arrayList.add(words[15]);

        arrayList.add(words[20]);
        words[20] = convertTime(words[20]);
        
        
        if(isValidDate(words[26]) && words[26].length() >= 6){
            words[26] = words[26].substring(0, 6);
        }
       
        words[24] = parseStreetName(words[24]);
        words[25] = parseStreetName(words[25]);

        arrayList.add(words[20]);
        arrayList.add(words[24]);
        arrayList.add(words[25]);
        arrayList.add(words[26]);

        words[31] = convertTime(words[31]);
        words[32] = convertTime(words[32]);

        arrayList.add(words[31]);
        arrayList.add(words[32]);

        words[35] = parseStringToInteger(words[35], 2023);

        words[37] = parseStringToInteger(words[37], 100);

        arrayList.add(words[35]);
        arrayList.add(words[37]);
        
        //String[] squished = Arrays.copyOf(words, words.length - 3);

        

        String result = String.join(",", arrayList);

        context.write(new Text(result),  NullWritable.get());
        
        
    }

    public static String getFirstTwoCharacters(String input) {
        if (input != null && input.length() >= 2) {
            return input.substring(0, 2);
        } else {
            // Handle cases where the input is null or has fewer than two characters
            return input;
        }
    }

    private static final Set<String> validStateCodes = new HashSet<>(Arrays.asList(
            "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA", "HI", "ID", "IL", "IN", "IA", "KS",
            "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ", "NM", "NY",
            "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY"
    ));


    public static boolean isValidStateCode(String input) {
        return input != null && validStateCodes.contains(input.toUpperCase());
    }

    private static String convertTime(String input){
        String trimmed = input.trim();
        String result = trimmed.replaceAll("\\s", "");
        if(result.length()<4){
            return "";
        }
        String hoursStr = result.substring(0, 2);
        String minutesStr = result.substring(2, 4);
        
        int hours, minutes;
        try {
            hours = Integer.parseInt(hoursStr);
            minutes = Integer.parseInt(minutesStr);
        }
        catch (NumberFormatException e) {
            return "";
        }

        if(hours > 24)
            return "";
        if(minutes>60)
            return "";

        if (result.length() == 6) {
           
            String amPmIndicator = result.substring(4, 6).toUpperCase();
            if ("PM".equals(amPmIndicator) && hours < 12) {
                hours += 12;
            } else if ("AM".equals(amPmIndicator) && hours == 12) {
                hours = 0;
            }
            else{
                return "";
            }

            return String.valueOf(hours) + String.valueOf(minutes);
        } else {
            // Invalid input, return original string or handle accordingly
            return result.substring(0, Math.min(input.length(), 4));
        }

    }

    private static String[] customSplit(String input, String delimiter) {
        // Using StringBuilder to build the result array
        StringBuilder element = new StringBuilder();
        List<String> result = new ArrayList<>();

        for (char c : input.toCharArray()) {
            if (String.valueOf(c).equals(delimiter)) {
                result.add(element.toString());
                element = new StringBuilder();
            } else {
                element.append(c);
            }
        }

        // Add the last element after the last delimiter
        result.add(element.toString());

        // Convert the list to an array
        return result.toArray(new String[0]);
    }

    public static String removeNonAlphanumeric(String input) {
        int startIndex = 0;
        for (int i = 0; i < input.length(); i++) {
            if (Character.isLetterOrDigit(input.charAt(i))) {
                startIndex = i;
                break;
            }
        }
        return input.substring(startIndex);
    }

    public static boolean isAlphabetic(String str) {
        for (int i = 0; i < str.length(); i++) {
          if (!Character.isAlphabetic(str.charAt(i))) {
            return false;
          }
        }
        return true;
      }

    public static String parseStringToInteger(String inputStr, int lim) {
        try {
            int result = Integer.parseInt(inputStr);
            if(result > lim){
                return "";
            }
            return String.valueOf(result);
        } catch (NumberFormatException e) {
            return "";
        }
    }

    public static String removeLastTick(String input) {
        int length = input.length();
        String ret = "";
        if(length >= 2 && input.charAt(length - 1) == '’') {
            ret = input.substring(0, length - 1);
        }
        else{
           ret = input;
        }
        return ret;
    }

    private static boolean isValidDate(String date) {

        try {
            if(date.length() < 8)
                return false;
            // Try parsing the date using Integer.parseInt
            int year = Integer.parseInt(date.substring(0, 4));
            int month = Integer.parseInt(date.substring(4, 6));
            int day = Integer.parseInt(date.substring(6, 8));

            // Check the range of year, month, and day
            if (year < 1000 || month < 1 || month > 12 || day < 1 || day > 31) {
                return false;
            }

            // You can add more specific checks if needed (e.g., check for valid days in each month)

        } catch (NumberFormatException e) {
            // If parsing fails, it's not a valid date
            return false;
        }

        // If all checks pass, the date is valid
        return true;
    }


    public static String parseStreetName(String streetNametemp) {

        String streetName = streetNametemp.toLowerCase();
        // Remove "th" at the end of numbers
        streetName = streetName.replaceAll("(\\d+)th\\b", "$1");

        streetName = streetName.replaceAll("(\\d+)st\\b", "$1");

        streetName = streetName.replaceAll("(\\d+)rd\\b", "$1");

        // Shorten street, str, or similar acronyms
        streetName = streetName.replaceAll("\\b(street|str)\\b", "s");

        // Shorten avenue to "a"
        streetName = streetName.replaceAll("\\bavenue\\b", "a");

        // Shorten road to "rd"
        streetName = streetName.replaceAll("\\broad\\b", "rd");

        // Remove "nd" at the end of numbers
        streetName = streetName.replaceAll("(\\d+)nd\\b", "$1");

        return streetName;
    }
} 
            


