package org.dataone.cn.index.util;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.dataone.configuration.Settings;

public class PerformanceLogger {

    private static PerformanceLogger self = new PerformanceLogger(); // non-lazy singleton 
    private static final Level LOGGING_LEVEL = Level.INFO;
    private static Logger perfLogger;
    private static boolean enabled;
    
    private PerformanceLogger() {
        enabled = Settings.getConfiguration().getBoolean("dataone.indexing.performance.logging.enabled", Boolean.FALSE);
        perfLogger = Logger.getLogger("performanceStats");
        
        System.out.println("PerformanceLogger : enabled=" + enabled + " appender=" + perfLogger);
        
        if (perfLogger == null) {
            Logger defaultLogger = Logger.getLogger(PerformanceLogger.class.getName());
            defaultLogger.error("Unable to create Logger for performanceStats appender!");
            enabled = false;
        }
    }
    
    public static PerformanceLogger getInstance() {
        return self;
    }
    
    public void log(String id, long milliseconds) {
        if (enabled)
            log("" + id + ", " + milliseconds);
    }
    
    public void log(String message) {
        System.out.println("PerformanceLogger.log : enabled=" + enabled + " appender=" + perfLogger);
        if (enabled)
            perfLogger.log(LOGGING_LEVEL, message);
    }
}
