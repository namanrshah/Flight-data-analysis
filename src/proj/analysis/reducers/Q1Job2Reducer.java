/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package proj.analysis.reducers;

import proj.analysis.util.Constants;
import proj.analysis.util.Q1CustomDataType;
import proj.analysis.util.ValueComparatorIntToFloat;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author namanrs
 */
public class Q1Job2Reducer extends Reducer<NullWritable, Text, NullWritable, Text> {

    Map<Integer, Float> valuesForBestTime = new HashMap<>();
    Map<Integer, Float> valuesForBestDay = new HashMap<>();
    Map<Integer, Float> valuesForBestMonth = new HashMap<>();

    @Override
    protected void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            String valueString = value.toString();
//            String[] split = valueString.split("\t");
//            String keyAndValue = split[1];
            String[] analysisKey = valueString.split(Constants.SEPARATORS.QUESTION_1_SEPARATOR);
            String keyForAnalysis = analysisKey[0].substring(1);
            String analysisType = analysisKey[0].substring(0, 1);
            if (analysisType.equals(Constants.KEY_INITIALS.Q12_HOUR_FOR_TIME)) {
                valuesForBestTime.put(Integer.parseInt(keyForAnalysis), Float.parseFloat(analysisKey[1]));
            } else if (analysisType.equals(Constants.KEY_INITIALS.Q12_DAY_OF_WEEK)) {
                valuesForBestDay.put(Integer.parseInt(keyForAnalysis), Float.parseFloat(analysisKey[1]));
            } else if (analysisType.equals(Constants.KEY_INITIALS.Q12_MONTH)) {
                valuesForBestMonth.put(Integer.parseInt(keyForAnalysis), Float.parseFloat(analysisKey[1]));
            }
        }
        ValueComparatorIntToFloat comparatorForBestTime = new ValueComparatorIntToFloat(valuesForBestTime);
        TreeMap sortedMapForBestTime = new TreeMap(comparatorForBestTime);
        sortedMapForBestTime.putAll(valuesForBestTime);
        context.write(NullWritable.get(), new Text(String.format("%-30s%-10s%-10s", "Best time to minimize delay ", sortedMapForBestTime.firstKey() + " - " + ((Integer) (sortedMapForBestTime.firstKey()) + 1), valuesForBestTime.get(sortedMapForBestTime.firstKey()))));
        ValueComparatorIntToFloat comparatorForBestDay = new ValueComparatorIntToFloat(valuesForBestDay);
        TreeMap sortedMapForBestDay = new TreeMap(comparatorForBestDay);
        sortedMapForBestDay.putAll(valuesForBestDay);
        context.write(NullWritable.get(), new Text(String.format("%-30s%-10s%-10s", "Best day to minimize delay ", sortedMapForBestDay.firstKey(), valuesForBestDay.get(sortedMapForBestDay.firstKey()))));
        ValueComparatorIntToFloat comparatorForBestMonth = new ValueComparatorIntToFloat(valuesForBestMonth);
        TreeMap sortedMapForBestMonth = new TreeMap(comparatorForBestMonth);
        sortedMapForBestMonth.putAll(valuesForBestMonth);
        context.write(NullWritable.get(), new Text(String.format("%-30s%-10s%-10s", "Best month to minimize delay ", sortedMapForBestMonth.firstKey(), valuesForBestMonth.get(sortedMapForBestMonth.firstKey()))));
        context.write(NullWritable.get(), new Text(String.format("%-30s%-10s%-10s", "Best time to maximize delay ", sortedMapForBestTime.lastKey() + " - " + ((Integer) (sortedMapForBestTime.lastKey()) + 1) % 24, valuesForBestTime.get(sortedMapForBestTime.lastKey()))));
        context.write(NullWritable.get(), new Text(String.format("%-30s%-10s%-10s", "Best day to maximize delay ", sortedMapForBestDay.lastKey(), valuesForBestDay.get(sortedMapForBestDay.lastKey()))));
        context.write(NullWritable.get(), new Text(String.format("%-30s%-10s%-10s", "Best month to maximize delay ", sortedMapForBestMonth.lastKey(), valuesForBestMonth.get(sortedMapForBestMonth.lastKey()))));
    }
}
