/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
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
public class Q3Job2Reducer extends Reducer<NullWritable, Text, NullWritable, Text> {

    @Override
    protected void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Map<String, Integer> flightFreq = new HashMap<>();
        context.write(NullWritable.get(), new Text("Yearwise trend : "));
        context.write(NullWritable.get(), new Text(String.format("%-10s%-10s%-10s%-10s%-10s%-10s%-10s%-10s%-10s%-10s%-10s", "Year", "Rank1", "Rank2", "Rank3", "Rank4", "Rank5", "Rank6", "Rank7", "Rank8", "Rank9", "Rank10")));
        for (Text value : values) {
            String valueString = value.toString();
            if (valueString.startsWith(Constants.KEY_INITIALS.Q3_YEAR)) {
                //Simply write to context                
                String[] splitValues = valueString.split(Constants.SEPARATORS.QUESTION_3_SEPARATOR);
                context.write(NullWritable.get(), new Text(String.format("%-10s%-10s%-10s%-10s%-10s%-10s%-10s%-10s%-10s%-10s%-10s", splitValues[0].substring(Constants.KEY_INITIALS.Q3_YEAR.length()), splitValues[1], splitValues[2], splitValues[3], splitValues[4], splitValues[5], splitValues[6], splitValues[7], splitValues[8], splitValues[9], splitValues[10])));
            } else if (valueString.startsWith(Constants.KEY_INITIALS.Q3_AIRPORT)) {
                //Find top 10 and write to context
                String[] splitKeyValue = valueString.split(Constants.SEPARATORS.QUESTION_3_SEPARATOR);
                flightFreq.put(splitKeyValue[0], Integer.parseInt(splitKeyValue[1]));
            }
        }
        

        ValueComparatorStringToInt comparator = new ValueComparatorStringToInt(flightFreq);
        TreeMap sortedMap = new TreeMap(comparator);
        sortedMap.putAll(flightFreq);
        List<String> sortedKeys = new ArrayList(sortedMap.keySet());
        context.write(NullWritable.get(), new Text("Overall busiest : "));
        for (int i = 0; i < 10; i++) {
            String keyToWrite = sortedKeys.get(i);
            Integer count = flightFreq.get(keyToWrite);
            context.write(NullWritable.get(), new Text(String.format("%-20s%-10s", keyToWrite, count.toString())));
        }
    }
}
