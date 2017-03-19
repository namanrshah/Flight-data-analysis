package proj.analysis.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 *
 * @author namanrs
 */
public class Q7Job1CustomDataType implements Writable {

    IntWritable depdelay;
    IntWritable lateAircraftDelay;
    IntWritable flightCount;
    IntWritable lateAircraftDelayCount;

    public Q7Job1CustomDataType() {
        depdelay = new IntWritable();
        lateAircraftDelay = new IntWritable();
        flightCount = new IntWritable();
        lateAircraftDelayCount = new IntWritable();
    }

    public Q7Job1CustomDataType(IntWritable depdelay, IntWritable sum, IntWritable count, IntWritable lateAircraftDelayCount) {
        this.depdelay = depdelay;
        this.lateAircraftDelay = sum;
        this.flightCount = count;
        this.lateAircraftDelayCount = lateAircraftDelayCount;
    }

    public IntWritable getDepdelay() {
        return depdelay;
    }

    public void setDepdelay(IntWritable depdelay) {
        this.depdelay = depdelay;
    }

    public IntWritable getLateAircraftDelay() {
        return lateAircraftDelay;
    }

    public void setLateAircraftDelay(IntWritable lateAircraftDelay) {
        this.lateAircraftDelay = lateAircraftDelay;
    }

    public IntWritable getFlightCount() {
        return flightCount;
    }

    public void setFlightCount(IntWritable flightCount) {
        this.flightCount = flightCount;
    }

    public IntWritable getLateAircraftDelayCount() {
        return lateAircraftDelayCount;
    }

    public void setLateAircraftDelayCount(IntWritable lateAircraftDelayCount) {
        this.lateAircraftDelayCount = lateAircraftDelayCount;
    }

    @Override
    public void write(DataOutput d) throws IOException {
        depdelay.write(d);
        lateAircraftDelay.write(d);
        flightCount.write(d);
        lateAircraftDelayCount.write(d);
    }

    @Override
    public void readFields(DataInput di) throws IOException {
        depdelay.readFields(di);
        lateAircraftDelay.readFields(di);
        flightCount.readFields(di);
        lateAircraftDelayCount.readFields(di);
    }
}
