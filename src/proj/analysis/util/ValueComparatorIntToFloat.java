package proj.analysis.util;

import java.util.Comparator;
import java.util.Map;

/**
 *
 * @author namanrs
 */
public class ValueComparatorIntToFloat implements Comparator {

    Map<Integer, Float> base;

    public ValueComparatorIntToFloat(Map<Integer, Float> base) {
        this.base = base;
    }

    public int compare(Object a, Object b) {
        if (base.get((Integer) a) >= base.get((Integer) b)) {
            return 1;
        } else {
            return -1;
        } // returning 0 would merge keys
    }

}
