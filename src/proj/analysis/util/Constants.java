/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package proj.analysis.util;

/**
 *
 * @author namanrs
 */
public class Constants {

    public static final String HEADER_STARTING_MAIN_DATA = "Year";
    public static final String HEADER_STARTING_PLANE_DATA = "tailnum";
    public static final String HEADER_STARTING_AIRPORTS_DATA = "\"iata\"";
    public static final String HEADER_STARTING_CARRIERS_DATA = "Code";
    public static final String NOT_APPLICALBLE = "NA";
    public static final String NONE = "None";
    public static final String ZERO = "0";
    public static final String ONE = "1";

    public static class SEPARATORS {

        public static final String DATA_FIELDS_SEPARATOR = ",";
        public static final String SUPP_DATA_FIELDS_SEPARATOR = "\"";
        public static final String QUESTION_1_SEPARATOR = "#";
        public static final String QUESTION_3_SEPARATOR = "#";
        public static final String QUESTION_3_FINAL_REDUCER_SEPARATOR = "@";
        public static final String QUESTION_4_SEPARATOR = "#";
        public static final String QUESTION_5_SEPARATOR = "#";
        public static final String QUESTION_6_SEPARATOR = "#";
        public static final String QUESTION_7_SEPARATOR = "#";
    }

    public static class FINAL_JOB_KEYS {

        public static final String QUESTION_1_KEY = "Q1,2";
    }

    public static class KEY_INITIALS {

        public static final String Q12_HOUR_FOR_TIME = "H";
        public static final String Q12_DAY_OF_WEEK = "W";
        public static final String Q12_MONTH = "M";
        public static final String Q3_YEAR = "YEAR";
        public static final String Q3_AIRPORT = "ARPRT";
    }

    public static class FILE_DATA_INITIALS {

        public static final char PLANE_DATA = 'N';
        public static final char OTHER_DATA = '\"';
    }
}
