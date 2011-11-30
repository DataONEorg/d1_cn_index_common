package org.dataone.cn.index.task;

import java.io.Serializable;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;

@Entity
@Table(name = "index_task")
public class IndexTask implements Serializable {

    /**
     * PK of index_task table
     */
    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    private Long id;

    /**
     * The object unique identifier
     */
    private String pid;

    /**
     * The object format id
     */
    private String formatid;

    /**
     * Filesystem path to a temporary cache of the system metadata as an XML
     * document on disk
     */
    private String sysMetaPath;

    /**
     * Filesystem path to the science metadata or resource map object. Null for
     * data objects
     */
    private String objectPath;

    /**
     * The dateSysMetadataModified of the system metadata
     */
    private long dateSysMetaModified;

    /**
     * task generated/modification date
     */
    private long taskModifiedDate;

    /**
     * Relative priority of this task. Some operations such as a change in
     * access control rules should be propagated to the index before others
     **/
    private int priority;

    /**
     * An indication that a particular task is being processed, etc. Will
     * indicate "NEW", "IN_PROCESS", "COMPLETE", "FAILED"
     * 
     * The tstamp should be updated when the status flag changes.
     */
    private String status;

    private static final String STATUS_NEW = "NEW";
    private static final String STATUS_IN_PROCESS = "IN PROCESS";
    private static final String STATUS_COMPLETE = "COMPLETE";
    private static final String STATUS_FAILED = "FAILED";

    public IndexTask() {
        this.taskModifiedDate = System.currentTimeMillis();
        this.status = STATUS_NEW;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getPid() {
        return pid;
    }

    public void setPid(String pid) {
        this.pid = pid;
    }

    public String getFormatid() {
        return formatid;
    }

    public void setFormatid(String formatid) {
        this.formatid = formatid;
    }

    public String getSysMetaPath() {
        return sysMetaPath;
    }

    public void setSysMetaPath(String sysMetaPath) {
        this.sysMetaPath = sysMetaPath;
    }

    public String getObjectPath() {
        return objectPath;
    }

    public void setObjectPath(String objectPath) {
        this.objectPath = objectPath;
    }

    public long getDateSysMetaModified() {
        return dateSysMetaModified;
    }

    public void setDateSysMetaModified(long dateSysMetaModified) {
        this.dateSysMetaModified = dateSysMetaModified;
    }

    public int getPriority() {
        return priority;
    }

    public void setPriority(int priority) {
        this.priority = priority;
    }

    public long getTaskModifiedDate() {
        return taskModifiedDate;
    }

    public void setTaskModifiedDate(long taskModifiedDate) {
        this.taskModifiedDate = taskModifiedDate;
    }

    @Override
    public String toString() {
        return "IndexTask [id=" + id + ", pid=" + pid + ", formatid=" + formatid + ", sysMetaPath="
                + sysMetaPath + ", objectPath=" + objectPath + ", dateSysMetaModified="
                + dateSysMetaModified + ", taskModifiedDate=" + taskModifiedDate + ", priority="
                + priority + ", status=" + status + "]";
    }

}