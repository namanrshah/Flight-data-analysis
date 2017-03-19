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
public class Q1CustomDataType implements Writable {

    IntWritable totalDelay;
    IntWritable count;
    IntWritable max;
    IntWritable cancelCount;

    public Q1CustomDataType() {
        this.cancelCount = new IntWritable();
        this.totalDelay = new IntWritable();
        this.count = new IntWritable();
        this.max = new IntWritable();
    }

    public Q1CustomDataType(IntWritable totalDelay, IntWritable count, IntWritable max, IntWritable cancelCount) {
        this.totalDelay = totalDelay;
        this.count = count;
        this.max = max;
        this.cancelCount = cancelCount;
    }

    
    public IntWritable getMax() {
        return max;
    }

    public void setMax(IntWritable max) {
        this.max = max;
    }

    public IntWritable getCancelCount() {
        return cancelCount;
    }

    public void setCancelCount(IntWritable cancelCount) {
        this.cancelCount = cancelCount;
    }

    public IntWritable getTotalDelay() {
        return totalDelay;
    }

    public void setTotalDelay(IntWritable totalDelay) {
        this.totalDelay = totalDelay;
    }

    public IntWritable getCount() {
        return count;
    }

    public void setCount(IntWritable count) {
        this.count = count;
    }

    @Override
    public void write(DataOutput d) throws IOException {
        totalDelay.write(d);
        max.write(d);
        count.write(d);
        cancelCount.write(d);
    }

    @Override
    public void readFields(DataInput di) throws IOException {
        totalDelay.readFields(di);
        max.readFields(di);
        count.readFields(di);
        cancelCount.readFields(di);
    }

}
