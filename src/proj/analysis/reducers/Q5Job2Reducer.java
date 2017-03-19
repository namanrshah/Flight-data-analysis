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
 * @author Rajiv
 */
public class Q5Job2Reducer extends Reducer<NullWritable, Text, NullWritable, Text> {

    @Override
    protected void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Map<String, Integer> carrierToDelaySum = new HashMap<>();
        Map<String, Integer> carrierToDelayCount = new HashMap<>();
        String highestAvgDelayCarrier = "";
        Float highestAvgDelay = 0.0f;
        for (Text value : values) {
            String valueString = value.toString();
            String[] splitData = valueString.split(Constants.SEPARATORS.QUESTION_5_SEPARATOR);
            String carrier = splitData[0];
            int delaySum = Integer.parseInt(splitData[1]);
            int delaycount = Integer.parseInt(splitData[2]);
            float avgDelay = delaySum * 1.0f / delaycount;
            if (avgDelay > highestAvgDelay) {
                highestAvgDelay = avgDelay;
                highestAvgDelayCarrier = carrier;
            }
            carrierToDelayCount.put(carrier, delaycount);
            carrierToDelaySum.put(carrier, delaySum);
        }
        context.write(NullWritable.get(), new Text("Question 5.\n"));
        ValueComparatorStringToInt delayCarrierCountComp = new ValueComparatorStringToInt(carrierToDelayCount);
        TreeMap sortedcarrierDelayCount = new TreeMap(delayCarrierCountComp);
        sortedcarrierDelayCount.putAll(carrierToDelayCount);
        context.write(NullWritable.get(), new Text("]\nOn basis of delayed flight count : "));
        context.write(NullWritable.get(), new Text("\n"));
        List<String> flightCounts = new ArrayList<>(sortedcarrierDelayCount.keySet());
        for (int i = 0; i < 10; i++) {
            context.write(NullWritable.get(), new Text(String.format("%-50s%-10s", flightCounts.get(i), carrierToDelayCount.get(flightCounts.get(i)))));
        }        
        ValueComparatorStringToInt delayFlightsSumMins = new ValueComparatorStringToInt(carrierToDelaySum);
        TreeMap sortedFlightDelaySumMins = new TreeMap(delayFlightsSumMins);
        sortedFlightDelaySumMins.putAll(carrierToDelaySum);
        context.write(NullWritable.get(), new Text("\nOn basis of total delay in minutes : "));
        context.write(NullWritable.get(), new Text("\n"));
        List<String> flightsDelayMins = new ArrayList<>(sortedFlightDelaySumMins.keySet());
        for (int i = 0; i < 10; i++) {
            context.write(NullWritable.get(), new Text(String.format("%-50s%-10s", flightsDelayMins.get(i), carrierToDelaySum.get(flightsDelayMins.get(i)))));
        }        
        context.write(NullWritable.get(), new Text("\nHighest average delay : " + highestAvgDelayCarrier + "\t" + Float.toString(highestAvgDelay)));
    }
}
