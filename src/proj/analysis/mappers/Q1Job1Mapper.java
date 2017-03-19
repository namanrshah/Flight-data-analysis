package proj.analysis.mappers;

import proj.analysis.util.Constants;
import proj.analysis.util.Q1CustomDataType;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author namanrs
 */
public class Q1Job1Mapper extends Mapper<LongWritable, Text, Text, Q1CustomDataType> {

    final IntWritable one = new IntWritable(1);
    final IntWritable zero = new IntWritable(0);

    @Override
    protected void map(LongWritable key, Text value, Context context) {
        String valueString = value.toString();
        if (!valueString.startsWith(Constants.HEADER_STARTING_MAIN_DATA)) {
            try {
                String[] dataValues = valueString.split(Constants.SEPARATORS.DATA_FIELDS_SEPARATOR);
                String month = dataValues[1];
                String dayOfWeek = dataValues[3];
                if (dataValues[5].length() >= 3) {
                    String schDepTimeHour = dataValues[5].substring(0, dataValues[5].length() - 2);
                    Q1CustomDataType customDataType = new Q1CustomDataType();
                    String cancelled = dataValues[21];
                    if (cancelled.equals(Constants.ZERO)) {
                        customDataType.setCancelCount(zero);
                    } else if (cancelled.equals(Constants.ONE)) {
                        customDataType.setCancelCount(one);
                    }
                    String arrivalDelay = dataValues[14];
                    String depDelay = dataValues[15];
                    if (arrivalDelay.equals(Constants.NOT_APPLICALBLE)) {
                        arrivalDelay = Constants.ZERO;
                    }
                    if (depDelay.equals(Constants.NOT_APPLICALBLE)) {
                        depDelay = Constants.ZERO;
                    }
                    customDataType.setTotalDelay(new IntWritable(Integer.parseInt(arrivalDelay) + Integer.parseInt(depDelay)));
                    customDataType.setMax(zero);
                    customDataType.setCount(one);
                    context.write(new Text(Constants.KEY_INITIALS.Q12_HOUR_FOR_TIME + schDepTimeHour), customDataType);
                    context.write(new Text(Constants.KEY_INITIALS.Q12_DAY_OF_WEEK + dayOfWeek), customDataType);
                    context.write(new Text(Constants.KEY_INITIALS.Q12_MONTH + month), customDataType);
                }
            } catch (IOException ex) {
                System.err.println("ERROR : Writing data for delay time.");
            } catch (InterruptedException ex) {
                System.err.println("ERROR : Writing data for delay time.");
            }
        }
    }
}
