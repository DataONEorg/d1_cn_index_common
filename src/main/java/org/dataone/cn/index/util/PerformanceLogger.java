package org.dataone.cn.index.util;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * Logging wrapper class for outputting performance/timing statistics. 
 */
public class PerformanceLogger {

    private static final Level LOG_LEVEL = Level.DEBUG;
    private Logger log = Logger.getLogger(PerformanceLogger.class.getName());
    
    
    /**
     * Logs the time spent against the task with the given id. 
     * @param id the String identifier for a task
     * @param time the time in milliseconds spent on the task 
     */
    public synchronized void logTime(String id, long time) {
        outputLog(String.format("%-50s, %20d", id, time));
    }
    
    private void outputLog(String logMessage) {
        log.log(LOG_LEVEL, logMessage);
    }
    
}
