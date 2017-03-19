package proj.analysis.mappers;

import proj.analysis.util.Constants;
import proj.analysis.util.Q5CustomDataType;
import proj.analysis.util.Q6Job1CustomDataType;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

/**
 *
 * @author namanrs
 */
public class Q6Job1Mapper extends Mapper<LongWritable, Text, Text, Q6Job1CustomDataType> {

    final IntWritable one = new IntWritable(1);
    final IntWritable zero = new IntWritable(0);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String valueString = value.toString();
        if (!valueString.startsWith(Constants.HEADER_STARTING_MAIN_DATA) && !valueString.startsWith(Constants.HEADER_STARTING_PLANE_DATA)) {
            String[] splitData = valueString.split(Constants.SEPARATORS.DATA_FIELDS_SEPARATOR);
            String firstProp = splitData[0];
            if (firstProp.charAt(0) == Constants.FILE_DATA_INITIALS.PLANE_DATA) {
                //its plane-data
                try {
                    String manuYear = splitData[8];
                    if (!manuYear.equals(Constants.NONE)) {
                        context.write(new Text(firstProp), new Q6Job1CustomDataType(new Text(manuYear), zero, zero));
                    }
                } catch (Exception ex) {
                    //No element on 8th position
                }
            } else {
                //its main data
                String tailNum = splitData[10];
                if (!tailNum.equals(Constants.NOT_APPLICALBLE)) {
//                    String arrivalDelay = splitData[14];
//                    String depDelay = splitData[15];
//                    if (!arrivalDelay.equals(Constants.NOT_APPLICALBLE) && !depDelay.equals(Constants.NOT_APPLICALBLE)) {
                    //                        int totalDelay = Integer.parseInt(arrivalDelay) + Integer.parseInt(depDelay);
                    String carrierDelay = splitData[24];
                    if (!carrierDelay.equals(Constants.NOT_APPLICALBLE)) {
                        int carrierDelayInt = Integer.parseInt(carrierDelay);
//                        if (totalDelay >= 0) {
                        context.write(new Text(tailNum), new Q6Job1CustomDataType(new Text(firstProp), new IntWritable(carrierDelayInt), one));
//                        }
                    }
                }
            }
        }
    }
}
