/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package proj.analysis.util;

import java.util.Comparator;
import java.util.Map;

/**
 *
 * @author Rajiv
 */
public class ValueComparatorStringToFloat implements Comparator {

    Map<String, Float> base;

    public ValueComparatorStringToFloat(Map<String, Float> base) {
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
