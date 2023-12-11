import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class stat_reducer extends Reducer<Text, DoubleWritable, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context)
            throws IOException, InterruptedException {

        List<Double> volList = new ArrayList<>();

        // Iterate through the 'Vol' values to collect them in a list
        for (DoubleWritable value : values) {
            volList.add(value.get());
        }

        // Calculate min, max, average, median, and standard deviation
        double minValue = Collections.min(volList);
        double maxValue = Collections.max(volList);
        double sum = 0;
        int count = 0;

        // Iterate through the 'Vol' values to calculate sum and count
        for (Double vol : volList) {
            sum += vol;
            count++;
        }

        // Calculate average
        double average = count > 0 ? sum / count : 0.0;

        // Calculate median
        double median = calculateMedian(volList);

        // Calculate standard deviation
        double stdDev = calculateStdDev(volList, average);

        // Output the results
        context.write(key, new Text(String.format("Min: %.2f, Max: %.2f, Avg: %.2f, Median: %.2f, StdDev: %.2f", minValue, maxValue, average, median, stdDev)));
    }

    private double calculateMedian(List<Double> volList) {
        // since after cleaning the data the vol of our dataset becomes more manageable
        // and the total list of values cab be easily sent as a whole and sorted
        Collections.sort(volList);

        int size = volList.size();
        if (size % 2 == 0) {
            // If the size is even, calculate the average of the two middle elements
            int mid1 = size / 2 - 1;
            int mid2 = size / 2;
            return (volList.get(mid1) + volList.get(mid2)) / 2.0;
        } else {
            // If the size is odd, return the middle element
            int mid = size / 2;
            return volList.get(mid);
        }
    }

    private double calculateStdDev(List<Double> volList, double mean) {
        double sumOfSquares = 0.0;

        for (double vol : volList) {
            sumOfSquares += (vol - mean) * (vol - mean);
        }

        return Math.sqrt(sumOfSquares / (volList.size() - 1));
    }
}
