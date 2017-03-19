package proj.analysis.util;

/**
 *
 * @author namanrs
 */
public class Q4CustomClass {

    int totalFlights;
    int actDelayedFlights;
    int delaySum;

    public Q4CustomClass() {
    }

    
    public Q4CustomClass(int totalFlights, int actDelayedFlights, int delaySum) {
        this.totalFlights = totalFlights;
        this.actDelayedFlights = actDelayedFlights;
        this.delaySum = delaySum;
    }
    
    

    public int getTotalFlights() {
        return totalFlights;
    }

    public void setTotalFlights(int totalFlights) {
        this.totalFlights = totalFlights;
    }

    public int getActDelayedFlights() {
        return actDelayedFlights;
    }

    public void setActDelayedFlights(int actDelayedFlights) {
        this.actDelayedFlights = actDelayedFlights;
    }

    public int getDelaySum() {
        return delaySum;
    }

    public void setDelaySum(int delaySum) {
        this.delaySum = delaySum;
    }

}
