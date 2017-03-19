/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package proj.analysis.reducers;

import proj.analysis.util.Constants;
import proj.analysis.util.Q1CustomDataType;
import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author namanrs
 */
public class Q1Job1Reducer extends Reducer<Text, Q1CustomDataType, NullWritable, Text> {

    @Override
    protected void reduce(Text key, Iterable<Q1CustomDataType> values, Context context) throws IOException, InterruptedException {
        int maxArrivalDelay = 0;
        int maxDepartureDelay = 0;
        int countCancelArrival = 0;
        int countCancelDep = 0;
        int sum = 0;
        int count = 0;
        for (Q1CustomDataType value : values) {
//            ++count;
//            String arrivalDelay = value.getCancelCount().toString();
//            String depDelay = value.getTotalDelay().toString();
//            if (!arrivalDelay.equals(Constants.NOT_APPLICALBLE)) {
//                int arrDelayInt = Integer.parseInt(arrivalDelay);
//                if (maxArrivalDelay < arrDelayInt) {
//                    maxArrivalDelay = arrDelayInt;
//                }
//                if (arrDelayInt < 0) {
//                    arrDelayInt = 0;
//                }
//                sum += arrDelayInt;
//            } else {
//                countCancelArrival++;
//            }
//            if (!depDelay.equals(Constants.NOT_APPLICALBLE)) {
//                int depDelayInt = Integer.parseInt(depDelay);
//                if (depDelayInt < 0) {
//                    depDelayInt = 0;
//                }
//                sum += depDelayInt;
//            } else {
//                countCancelDep++;
//            }
            sum += value.getTotalDelay().get();
            count += value.getCount().get();
            //penalty
            sum += value.getCancelCount().get() * value.getMax().get();
        }
//        sum += (countCancelArrival) * maxArrivalDelay;
//        sum += (countCancelDep) * maxDepartureDelay;
        context.write(NullWritable.get(), new Text(key + Constants.SEPARATORS.QUESTION_1_SEPARATOR + Float.toString(sum * 1.0f / count)));
    }
//}
//public class Q1Job1Reducer extends Reducer<Text, Q1CustomDataType, NullWritable, Text> {
//
//    @Override
//    protected void reduce(Text key, Iterable<Q1CustomDataType> values, Context context) throws IOException, InterruptedException {
//        int maxArrivalDelay = 0;
//        int maxDepartureDelay = 0;
//        int sum = 0;
//        int count = 0;
////        for (Text value : values) {
////            ++count;
////            String arrivalDelay = value.toString().split(",")[0];
////            String depDelay = value.toString().split(",")[1];
////            if (!arrivalDelay.equals(Constants.NOT_APPLICALBLE)) {
////                int arrDelayInt = Integer.parseInt(arrivalDelay);
////                int depDelayInt = Integer.parseInt(depDelay);
////                if (maxArrivalDelay < arrDelayInt) {
////                    maxArrivalDelay = arrDelayInt;
////                }
////                if (maxDepartureDelay < depDelayInt) {
////                    maxDepartureDelay = depDelayInt;
////                }
////            }
////        }
//        for (Q1CustomDataType value : values) {
////            String arrivalDelay = value.toString().split(",")[0];
////            String depDelay = value.toString().split(",")[1];
////            if (!arrivalDelay.equals(Constants.NOT_APPLICALBLE)) {
////                int arrDelayInt = Integer.parseInt(arrivalDelay);
////
////                if (arrDelayInt < 0) {
////                    arrDelayInt = 0;
////                }
////                sum += arrDelayInt;
////            } else {
////                sum += maxArrivalDelay;
////            }
////            if (!depDelay.equals(Constants.NOT_APPLICALBLE)) {
////                int depDelayInt = Integer.parseInt(depDelay);
////                if (depDelayInt < 0) {
////                    depDelayInt = 0;
////                }
////                sum += depDelayInt;
////            } else {
////                sum += maxDepartureDelay;
////            }
//
////            context.write(NullWritable.get(), new Text(value.getArrivalDelay().toString() + "," + value.getArrivalDelay().toString()));
//        }
//        for (Q1CustomDataType value : values) {
//            context.write(NullWritable.get(), new Text(value.getArrivalDelay().toString() + "," + value.getArrivalDelay().toString()));
//        }
////        for (Q1CustomDataType value : values) {
////            context.write(NullWritable.get(), key);
////        }
////        context.write(NullWritable.get(), new Text(key + Constants.SEPARATORS.QUESTION_1_SEPARATOR + Float.toString(sum * 1.0f / count)));
//    }
}
