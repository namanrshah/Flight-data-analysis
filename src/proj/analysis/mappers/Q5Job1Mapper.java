package proj.analysis.mappers;

import proj.analysis.util.Constants;
import proj.analysis.util.Q5CustomDataType;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author Rajiv
 */
public class Q5Job1Mapper extends Mapper<LongWritable, Text, Text, Q5CustomDataType> {

    final IntWritable one = new IntWritable(1);
    final IntWritable zero = new IntWritable(0);
    final Text blank = new Text("");

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//        String valueString = value.toString();
//        if (!valueString.startsWith(Constants.HEADER_STARTING_MAIN_DATA) && !valueString.startsWith(Constants.HEADER_STARTING_CARRIERS_DATA)) {
//            if (valueString.charAt(0) != Constants.FILE_DATA_INITIALS.OTHER_DATA) {
//                String[] dataValues = valueString.split(Constants.SEPARATORS.DATA_FIELDS_SEPARATOR);
////            String year = dataValues[0];
//                String carrier = dataValues[8];
//                String arrivalDelay = dataValues[14];
//                String depDelay = dataValues[15];
//                if (!carrier.equals(Constants.NOT_APPLICALBLE) && !arrivalDelay.equals(Constants.NOT_APPLICALBLE) && !depDelay.equals(Constants.NOT_APPLICALBLE)) {
//                    int arrDelayInt = Integer.parseInt(arrivalDelay);
//                    int depDelayInt = Integer.parseInt(depDelay);
//                    if (arrDelayInt > 0 || depDelayInt > 0) {
//                        if (arrDelayInt < 0) {
//                            arrDelayInt = 0;
//                        }
//                        if (depDelayInt < 0) {
//                            depDelayInt = 0;
//                        }
//                        context.write(new Text(carrier), new Q5CustomDataType(new IntWritable(depDelayInt + arrDelayInt), one, blank));
//                    }
//                }
//            } else {
//                String[] suppDataSplit = valueString.split(Constants.SEPARATORS.SUPP_DATA_FIELDS_SEPARATOR);
//                context.write(new Text(suppDataSplit[1]), new Q5CustomDataType(zero, zero, new Text(suppDataSplit[3])));
//            }
//        }
        String valueString = value.toString();
        if (!valueString.startsWith(Constants.HEADER_STARTING_MAIN_DATA) && !valueString.startsWith(Constants.HEADER_STARTING_CARRIERS_DATA)) {
            if (valueString.charAt(0) != Constants.FILE_DATA_INITIALS.OTHER_DATA) {
                String[] dataValues = valueString.split(Constants.SEPARATORS.DATA_FIELDS_SEPARATOR);
//            String year = dataValues[0];
                String carrier = dataValues[8];
                String arrivalDelay = dataValues[14];
                String depDelay = dataValues[15];
                if (!carrier.equals(Constants.NOT_APPLICALBLE) && !arrivalDelay.equals(Constants.NOT_APPLICALBLE) && !depDelay.equals(Constants.NOT_APPLICALBLE)) {
                    int arrDelayInt = Integer.parseInt(arrivalDelay);
                    int depDelayInt = Integer.parseInt(depDelay);
                    if (arrDelayInt > 0 || depDelayInt > 0) {
                        if (arrDelayInt < 0) {
                            arrDelayInt = 0;
                        }
                        if (depDelayInt < 0) {
                            depDelayInt = 0;
                        }
                        context.write(new Text(carrier), new Q5CustomDataType(new IntWritable(depDelayInt + arrDelayInt), one, blank));
                    }
                }
            } else {
                String[] suppDataSplit = valueString.split(Constants.SEPARATORS.SUPP_DATA_FIELDS_SEPARATOR);
                context.write(new Text(suppDataSplit[1]), new Q5CustomDataType(zero, zero, new Text(suppDataSplit[3])));
            }
        }
    }
}
