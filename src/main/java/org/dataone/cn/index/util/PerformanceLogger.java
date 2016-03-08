package org.dataone.cn.index.util;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.PriorityQueue;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * Logging wrapper class for tracking  that can be used to aggregate time spent in performing 
 * certain tasks, and for printing out statistics about this.
 */
public class PerformanceLogger {

    private static final Level LOG_LEVEL = Level.DEBUG;
    private Logger log = Logger.getLogger(PerformanceLogger.class.getName());
    
//    /** accumulated time spent by ID */
//    private HashMap<String,Long> timeById = new LinkedHashMap<String, Long>();
//    /** accumulated runs per ID */
//    private HashMap<String,Long> runsById = new HashMap<String, Long>();
//    /** total time across all IDs */
//    private long totalTime = 0;
    
    
    /**
     * Logs the time spent against the task with the given id, 
     * adding it to accumulated time and number of runs for the
     * task.
     * @param id the String identifier for a task
     * @param time the time in milliseconds spent on the task 
     */
    public synchronized void logTime(String id, long time) {
        
        outputLog(String.format("%-50s %20d", id, time));
        
//        Long storedTime = timeById.get(id);
//        if (storedTime == null)
//            storedTime = new Long(0);
//        storedTime = storedTime.longValue() + time;
//        timeById.put(id, storedTime);
//        
//        Long runs = runsById.get(id);
//        if (runs == null)
//            runs = new Long(0);
//        runs = runs.longValue() + 1;
//        runsById.put(id, runs);
    }
    
    private void outputLog(String logMessage) {
        log.log(LOG_LEVEL, logMessage);
    }
    
//    /**
//     * Returns the total amount of time (in milliseconds) spent on
//     * the task with the given id.
//     * @param id the String identifier for the task
//     * @return total time (in milliseconds) spent on the task
//     */
//    public synchronized long getTimeForId(String id) {
//        return timeById.get(id);
//    }
//    
//    /**
//     * Returns the number of runs logged for the task with the 
//     * given id.
//     * @param id the String identifier for the task
//     * @return amount of time (in milliseconds) spent on the task
//     */
//    public synchronized long getRunsForId(String id) {
//        return runsById.get(id);
//    }
//    
//    /**
//     * Returns the total amount of time (in milliseconds) logged
//     * against all tasks.
//     * @return total time logged (in milliseconds) for all tasks
//     */
//    public synchronized long getTotalTime() {
//        return totalTime;
//    }
//    
//    /**
//     * Outputs statistics accross all runs of {@link #logTime(String, long)}.
//     * Includes total time logged for all tasks, and time, number of runs, plus
//     * average time per run for each task id. Also outputs task ids in order 
//     * of their average time.
//     */
//    public synchronized void outputStatistics() {
//        
//        PriorityQueue<TimeIdPair> idQueue = new PriorityQueue<TimeIdPair>();
//        
//        // calculating average time for each ID
//        HashMap<String, Float> avgTimeById = new HashMap<String, Float>();
//        float totalOfAverages = 0.0f;
//        for (String id : timeById.keySet()) {
//            Long time = timeById.get(id);
//            Long runs = runsById.get(id);
//            float avgTime = (float) time / runs;
//            avgTimeById.put(id, avgTime);
//            idQueue.add(new TimeIdPair(avgTime, id));
//            totalOfAverages += avgTime;
//        }
//        
//        outputLog("============================================================");
//        outputLog("=====   Statistics: ========================================");
//        
//        // output total time
//        float secondsInAllIds = (float) totalTime / 1000;
//        outputLog(String.format("%-40s %s" , "Total time: ", "" + secondsInAllIds));
//
//        // output statistics for each id
//        for (String id : timeById.keySet()) {
//            outputLog("-----   Statistics for :  " + String.format("%-20s", id) + "   -----------");
//            outputLog(String.format("%-50s %10s" , "Time: ", "" + timeById.get(id)));
//            outputLog(String.format("%-50s %10s" , "Number of runs: ", "" + runsById.get(id)));
//            outputLog(String.format("%-50s %10s" , "Average time per run: ", "" + avgTimeById.get(id)));
//            // outputLog(String.format("%-40s= %s" , "% of time vs others : ", "" + avgTimeById.get(id) / totalOfAverages));
//            // ^    this last one assumes that id's are mutually exclusive
//            //      but some may be logged within others... 
//        }
//        
//        outputLog("----- In order of avgerage time : --------------------------");
//        while(!idQueue.isEmpty()) {
//            TimeIdPair timeId = idQueue.poll();
//            outputLog(String.format("%-50s %10s" , timeId.id + ": ", "" + timeId.t));
//        }
//        outputLog("============================================================");
//    }
//
//    /**
//     * Clears statistics logged by all calls to {@link #logTime(String, long)}
//     * (time per task, number of runs, and total time).
//     */
//    public synchronized void clear() {
//        timeById = new HashMap<String, Long>();
//        runsById = new HashMap<String, Long>();
//        totalTime = 0;
//    }
//    
//    /**
//     * Pairs task id with its time so we can insert them into a heap /
//     * PriorityQueue.
//     */
//    private static class TimeIdPair implements Comparable<TimeIdPair> {
//        public float t = 0;
//        public String id = "";
//        
//        public TimeIdPair(float t, String id) {
//            this.t = t;
//            this.id = id;
//        }
//
//        @Override
//        public int compareTo(TimeIdPair o) {
//            if (t < ((TimeIdPair)o).t)
//                return -1;
//            if (((TimeIdPair)o).t > t)
//                return 1;
//            return 0;
//        }
//    }
}
