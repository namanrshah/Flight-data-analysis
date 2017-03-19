package proj.analysis.reducers;

import proj.analysis.util.Constants;
import proj.analysis.util.ValueComparatorStringToFloat;
import proj.analysis.util.ValueComparatorStringToInt;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author namanrs
 */
public class Q7Job2Reducer extends Reducer<NullWritable, Text, NullWritable, Text> {

    final IntWritable one = new IntWritable(1);

    @Override
    protected void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int totDepDelay = 0;
        int totLateAircraftDelay = 0;
        int totalLateAC = 0;
        Map<String, Integer> airportToLateACDelay = new HashMap<>();
        Map<String, Float> airportToLateCount = new HashMap<>();
        Map<String, Float> airportToPercLateAC = new HashMap<>();
        for (Text value : values) {
            String valString = value.toString();
            String[] splitdata = valString.split(Constants.SEPARATORS.QUESTION_7_SEPARATOR);
            String airport = splitdata[0];
            int depDelay = Integer.parseInt(splitdata[1]);
            totDepDelay += depDelay;
            int lateAircraftDelay = Integer.parseInt(splitdata[2]);
            String flightCountStr = splitdata[3];
            int flightCountInt = Integer.parseInt(flightCountStr);
            String lateACCountStr = splitdata[4];
            int lateACCountInt = Integer.parseInt(lateACCountStr);
            totalLateAC += lateACCountInt;
            totLateAircraftDelay += lateAircraftDelay;
            airportToLateCount.put(airport, (float) lateACCountInt);
            airportToLateACDelay.put(airport, lateAircraftDelay);
            airportToPercLateAC.put(airport, lateACCountInt * 100.0f / flightCountInt);
        }
        ValueComparatorStringToInt comparator = new ValueComparatorStringToInt(airportToLateACDelay);
        TreeMap sortedMap = new TreeMap(comparator);
        sortedMap.putAll(airportToLateACDelay);
        List<String> sortedKeys = new ArrayList(sortedMap.keySet());
        for (int i = 0; i < 20; i++) {
            String ap = sortedKeys.get(i);
            Integer delay = airportToLateACDelay.get(ap);
            context.write(NullWritable.get(), new Text(String.format("%-10s%-15s%-15s", ap, delay, delay * 100.0f / totLateAircraftDelay)));
        }

        for (Map.Entry<String, Float> entrySet : airportToLateCount.entrySet()) {
            String airport = entrySet.getKey();
            Float lateCount = entrySet.getValue();
            airportToLateCount.put(airport, lateCount * 100.0f / totalLateAC);
        }
//        ValueComparatorStringToFloat comparatorFlt = new ValueComparatorStringToFloat(airportToPercLateAC);
//        TreeMap sortedMapflt = new TreeMap(comparatorFlt);
//        sortedMapflt.putAll(airportToPercLateAC);
//        List<String> sortedKeysflt = new ArrayList(sortedMapflt.keySet());
//        for (int i = 0; i < 20; i++) {
//            String ap = sortedKeysflt.get(i);
//            Float perc = airportToPercLateAC.get(ap);
//            context.write(NullWritable.get(), new Text(String.format("%-10s%-15s", ap, perc)));
//        }

        ValueComparatorStringToFloat comparatorFlt = new ValueComparatorStringToFloat(airportToLateCount);
        TreeMap sortedMapflt = new TreeMap(comparatorFlt);
        sortedMapflt.putAll(airportToLateCount);
        List<String> sortedKeysflt = new ArrayList(sortedMapflt.keySet());
        for (int i = 0; i < 20; i++) {
            String ap = sortedKeysflt.get(i);
            Float perc = airportToLateCount.get(ap);
            context.write(NullWritable.get(), new Text(String.format("%-10s%-15s", ap, perc)));
        }
        context.write(NullWritable.get(), new Text("\n"));
        context.write(NullWritable.get(), new Text(totLateAircraftDelay + "," + totDepDelay));
        context.write(NullWritable.get(), new Text(Float.toString(totLateAircraftDelay * 100.0f / totDepDelay)));
    }
}
