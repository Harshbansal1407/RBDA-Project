import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.util.Scanner;

import javax.swing.text.DefaultStyledDocument.ElementSpec;  

import java.util.ArrayList;
import java.util.List;

public class WordCountMapper
    extends Mapper<LongWritable, Text, Text, IntWritable> {
    private static final int MISSING = 9999;

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();

        String[] words = customSplit(line, ","); 
        int idx = 37;
        
        int length = words.length;
    
        if (idx < length) {
            String n = words[idx];
            context.write(new Text(words[idx]), new IntWritable(1));
            
        }
        else{
            context.write(new Text(""), new IntWritable(1));
        }
        /* 
        for (String token : words) {
            if (!token.trim().isEmpty()) {
                String interWord = removeLastTick(removeNonAlphanumeric(token));
                if ( !interWord.trim().isEmpty() && isAlphabetic(interWord) ){
                    int v = 1;              
                    String finalToken = interWord.toLowerCase();
                    context.write(new Text(finalToken), new IntWritable(v));
                }
            }
            
        }
        */
        
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

} 
