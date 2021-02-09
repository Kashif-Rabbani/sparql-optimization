package dk.uni.cs.utils;

import org.apache.log4j.Logger;

public class Log {
    public static final boolean LOGGING_SWITCH = false;
    final static org.apache.log4j.Logger logger = Logger.getLogger(Log.class);
    
    public static void consolePrint(String value) {
        System.out.println(value);
    }
    
    public static void log(String value) {
        if (LOGGING_SWITCH)
            System.out.print(value);
    }
    
    public static void logLine(String value) {
        if (LOGGING_SWITCH)
            System.out.println(value);
    }
    
}
