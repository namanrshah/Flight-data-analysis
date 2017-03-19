package proj.analysis.reducers;

import proj.analysis.util.Constants;
import proj.analysis.util.ValueComparatorStringToInt;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author namanrs
 */
public class Q3NewJob2Reducer extends Reducer<NullWritable, Text, NullWritable, Text> {

    @Override
    protected void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Map<String, Integer> airportFreq = new HashMap<>();
        Map<Integer, Map<String, Integer>> yearwiseAirportFreq = new TreeMap<>();
        context.write(NullWritable.get(), new Text("Yearwise trend : "));
        context.write(NullWritable.get(), new Text(String.format("%-10s%-50s%-50s%-50s%-50s%-50s%-50s%-50s%-50s%-50s%-50s", "Year", "Rank1", "Rank2", "Rank3", "Rank4", "Rank5", "Rank6", "Rank7", "Rank8", "Rank9", "Rank10")));
        for (Text value : values) {
            String valueString = value.toString();
            String[] splitData = valueString.split(Constants.SEPARATORS.QUESTION_3_SEPARATOR);
            String airport = splitData[0];
            Integer freq = Integer.parseInt(splitData[1]);
            airportFreq.put(airport, freq);
            int len = splitData.length;
            if (len > 2) {
                for (int i = 2; i < len; i++) {
                    String yearToCount = splitData[i];
                    String[] yearandCount = yearToCount.split(Constants.SEPARATORS.QUESTION_3_FINAL_REDUCER_SEPARATOR);
                    int year = Integer.parseInt(yearandCount[0]);
                    int count = Integer.parseInt(yearandCount[1]);
                    Map<String, Integer> airportFreqTemp = yearwiseAirportFreq.get(year);
                    if (airportFreqTemp == null) {
                        airportFreqTemp = new HashMap<>();
                        yearwiseAirportFreq.put(year, airportFreqTemp);
                    }
                    airportFreqTemp.put(airport, count);
                }
            }
        }
        for (Map.Entry<Integer, Map<String, Integer>> entrySet : yearwiseAirportFreq.entrySet()) {
            Integer year = entrySet.getKey();
            Map<String, Integer> apFreq = entrySet.getValue();
            ValueComparatorStringToInt comp = new ValueComparatorStringToInt(apFreq);
            TreeMap sortedMap = new TreeMap(comp);
            sortedMap.putAll(apFreq);
            List<String> sortedKeys = new ArrayList<>(sortedMap.keySet());
            List<String> vals = new ArrayList<>();
            for (int i = 0; i < 10; i++) {
                String keyToWrite = sortedKeys.get(i);
                Integer count = apFreq.get(keyToWrite);
//                vals.add(keyToWrite.substring(0, keyToWrite.indexOf("-")) + "(" + count + ")");
                vals.add(keyToWrite + " (" + count + ")");
            }
            context.write(NullWritable.get(), new Text(String.format("%-10s%-50s%-50s%-50s%-50s%-50s%-50s%-50s%-50s%-50s%-50s", year, vals.get(0), vals.get(1), vals.get(2), vals.get(3), vals.get(4), vals.get(5), vals.get(6), vals.get(7), vals.get(8), vals.get(9))));
        }
        ValueComparatorStringToInt comparator = new ValueComparatorStringToInt(airportFreq);
        TreeMap sortedMap = new TreeMap(comparator);
        sortedMap.putAll(airportFreq);
        List<String> sortedKeys = new ArrayList(sortedMap.keySet());
        context.write(NullWritable.get(), new Text("\nOverall busiest : "));
        for (int i = 0; i < 10; i++) {
            String keyToWrite = sortedKeys.get(i);
            Integer count = airportFreq.get(keyToWrite);
            context.write(NullWritable.get(), new Text(String.format("%-50s%-10s", keyToWrite, "(" + count.toString() + ")")));
        }
    }
}
