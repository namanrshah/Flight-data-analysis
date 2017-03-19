package proj.analysis.util;

import java.util.Comparator;
import java.util.Map;

/**
 *
 * @author namanrs
 */
public class ValueComparatorStringToInt implements Comparator {

    Map<String, Integer> base;

    public ValueComparatorStringToInt(Map<String, Integer> base) {
        this.base = base;
    }

    public int compare(Object a, Object b) {
        if (base.get((String) a) >= base.get((String) b)) {
            return -1;
        } else {
            return 1;
        } // returning 0 would merge keys
    }
}
