package proj.analysis.reducers;

import proj.analysis.util.Constants;
import proj.analysis.util.Q4CustomClass;
import proj.analysis.util.ValueComparatorStringToFloat;
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
public class Q4Job2Reducer extends Reducer<NullWritable, Text, NullWritable, Text> {

    @Override
    protected void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Map<String, Float> airportToFlightPercent = new HashMap<>();
        Map<String, Integer> airportToDelayedFlightCount = new HashMap<>();
        Map<String, Integer> airportToDelaySum = new HashMap<>();
        Map<String, Float> airportToAvgDelayMins = new HashMap<>();
        Map<String, Q4CustomClass> citytoStats = new HashMap<>();
        for (Text value : values) {
            String valueString = value.toString();
            String[] splitData = valueString.split(Constants.SEPARATORS.QUESTION_4_SEPARATOR);
            String city = splitData[0];
            int totalFlights = Integer.parseInt(splitData[1]);
            int actDelayedFlights = Integer.parseInt(splitData[2]);
            int delaySum = Integer.parseInt(splitData[3]);
            Q4CustomClass stats = citytoStats.get(city);
            if (stats == null) {
                stats = new Q4CustomClass(0, 0, 0);
                citytoStats.put(city, stats);
            }
            stats.setActDelayedFlights(stats.getActDelayedFlights() + actDelayedFlights);
            stats.setDelaySum(stats.getDelaySum() + delaySum);
            stats.setTotalFlights(stats.getTotalFlights() + totalFlights);
//            airportToAvgDelayMins.put(city, delaySum * 1.0f / actDelayedFlights);
//            airportToDelaySum.put(city, delaySum);
//            airportToDelayedFlightCount.put(city, actDelayedFlights);
//            airportToFlightPercent.put(city, actDelayedFlights * 100.0f / totalFlights);
        }
        for (Map.Entry<String, Q4CustomClass> entrySet : citytoStats.entrySet()) {
            String city = entrySet.getKey();
            Q4CustomClass stats = entrySet.getValue();
            if (stats.getActDelayedFlights() != 0 && stats.getDelaySum() != 0) {
//                airportToAvgDelayMins.put(city, stats.getDelaySum() * 1.0f / stats.getActDelayedFlights());
                airportToAvgDelayMins.put(city, stats.getDelaySum() * 1.0f / stats.getTotalFlights());
            }
            airportToDelaySum.put(city, stats.getDelaySum());
            airportToDelayedFlightCount.put(city, stats.getActDelayedFlights());
            if (stats.getActDelayedFlights() != 0 && stats.getTotalFlights() != 0) {
                airportToFlightPercent.put(city, stats.getActDelayedFlights() * 100.0f / stats.getTotalFlights());
            }
        }
        context.write(NullWritable.get(), new Text("Question 4."));
        ValueComparatorStringToFloat delayFlightsPercent = new ValueComparatorStringToFloat(airportToFlightPercent);
        TreeMap sortedDelayedFlghtPercent = new TreeMap(delayFlightsPercent);
        sortedDelayedFlghtPercent.putAll(airportToFlightPercent);
        context.write(NullWritable.get(), new Text("\nOn basis of percent of delayed flights : \n"));
        List<String> delayedFlightsPercent = new ArrayList<>(sortedDelayedFlghtPercent.keySet());
        for (int i = 0; i < 10; i++) {
            context.write(NullWritable.get(), new Text(String.format("%-30s%-10s", delayedFlightsPercent.get(i), airportToFlightPercent.get(delayedFlightsPercent.get(i)))));
        }
        ValueComparatorStringToFloat delayFlightsAvgMins = new ValueComparatorStringToFloat(airportToAvgDelayMins);
        TreeMap sortedDelayedFlghtAvgMins = new TreeMap(delayFlightsAvgMins);
        sortedDelayedFlghtAvgMins.putAll(airportToAvgDelayMins);
        context.write(NullWritable.get(), new Text("\nOn basis of average delay in minutes : \n"));
        List<String> delayedFlightsAvgDelay = new ArrayList<>(sortedDelayedFlghtAvgMins.keySet());
        for (int i = 0; i < 10; i++) {
            context.write(NullWritable.get(), new Text(String.format("%-30s%-10s", delayedFlightsAvgDelay.get(i), airportToAvgDelayMins.get(delayedFlightsAvgDelay.get(i)))));
        }
        ValueComparatorStringToInt delayFlightCountComp = new ValueComparatorStringToInt(airportToDelayedFlightCount);
        TreeMap sortedFlightCount = new TreeMap(delayFlightCountComp);
        sortedFlightCount.putAll(airportToDelayedFlightCount);
        context.write(NullWritable.get(), new Text("\nOn basis of delayed flight count : \n"));
        List<String> flightCounts = new ArrayList<>(sortedFlightCount.keySet());
        for (int i = 0; i < 10; i++) {
            context.write(NullWritable.get(), new Text(String.format("%-30s%-10s", flightCounts.get(i), airportToDelayedFlightCount.get(flightCounts.get(i)))));
        }
        ValueComparatorStringToInt delayFlightsSumMins = new ValueComparatorStringToInt(airportToDelaySum);
        TreeMap sortedFlightDelaySumMins = new TreeMap(delayFlightsSumMins);
        sortedFlightDelaySumMins.putAll(airportToDelaySum);
        context.write(NullWritable.get(), new Text("\nOn basis of total delay in minutes : \n"));
        List<String> flightsDelayMins = new ArrayList<>(sortedFlightDelaySumMins.keySet());
        for (int i = 0; i < 10; i++) {
            context.write(NullWritable.get(), new Text(String.format("%-30s%-10s", flightsDelayMins.get(i), airportToDelaySum.get(flightsDelayMins.get(i)))));
        }
    }
}
