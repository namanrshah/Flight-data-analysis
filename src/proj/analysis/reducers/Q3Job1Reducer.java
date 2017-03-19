/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package proj.analysis.reducers;

import proj.analysis.util.Constants;
import proj.analysis.util.Q3CustomDataType;
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
public class Q3Job1Reducer extends Reducer<Text, Q3CustomDataType, NullWritable, Text> {

    @Override
    protected void reduce(Text key, Iterable<Q3CustomDataType> values, Context context) throws IOException, InterruptedException {
        String keyString = key.toString();
        if (keyString.startsWith(Constants.KEY_INITIALS.Q3_YEAR)) {
            Map<String, Integer> flightFreq = new HashMap<>();
            for (Q3CustomDataType value : values) {
                String airport = value.getProperty().toString();
                int countToAdd = value.getCount().get();
                Integer count = flightFreq.get(airport);
                if (count == null) {
                    count = 0;
                }
                flightFreq.put(airport, count + countToAdd);
            }
//            sort and write first 10
            ValueComparatorStringToInt comparator = new ValueComparatorStringToInt(flightFreq);
            TreeMap sortedVals = new TreeMap(comparator);
            sortedVals.putAll(flightFreq);
            List keySet = new ArrayList(sortedVals.keySet());
            String strToWrite = "";
            for (int i = 0; i < 10; i++) {
                int freq = flightFreq.get(keySet.get(i));
                if (i < 9) {
                    strToWrite += keySet.get(i) + Constants.SEPARATORS.QUESTION_3_FINAL_REDUCER_SEPARATOR + freq + Constants.SEPARATORS.QUESTION_3_SEPARATOR;
                } else {
                    strToWrite += keySet.get(i) + Constants.SEPARATORS.QUESTION_3_FINAL_REDUCER_SEPARATOR + freq;
                }
            }
            context.write(NullWritable.get(), new Text(key + Constants.SEPARATORS.QUESTION_3_SEPARATOR + strToWrite));
        } else if (keyString.startsWith(Constants.KEY_INITIALS.Q3_AIRPORT)) {
            int count = 0;
            for (Q3CustomDataType value : values) {
                count += value.getCount().get();
            }
            context.write(NullWritable.get(), new Text(key + Constants.SEPARATORS.QUESTION_3_SEPARATOR + Integer.toString(count)));
        }
    }
}
