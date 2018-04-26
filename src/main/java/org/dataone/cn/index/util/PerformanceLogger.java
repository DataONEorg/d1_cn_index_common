package org.dataone.cn.index.util;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.dataone.configuration.Settings;

public class PerformanceLogger {

    static final Logger defaultLogger = Logger.getLogger(PerformanceLogger.class);
	
    private static PerformanceLogger self = new PerformanceLogger(); // non-lazy singleton 
    private static final Level LOGGING_LEVEL = Level.INFO;
    private static Logger perfLogger;
    private static boolean enabled;

    
    private PerformanceLogger() {
        enabled = Settings.getConfiguration().getBoolean("dataone.indexing.performance.logging.enabled", Boolean.FALSE);
        defaultLogger.warn("Setting up PerformanceLogger: set to enabled? " + enabled);
        
        perfLogger = Logger.getLogger("performanceStats");
        
        if (perfLogger == null) {
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
        if (enabled)
            perfLogger.log(LOGGING_LEVEL, message);
    }
    
    public boolean isLogEnabled() {
    	return enabled;
    }
}
