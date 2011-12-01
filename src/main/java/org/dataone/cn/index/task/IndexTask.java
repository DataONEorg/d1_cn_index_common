package org.dataone.cn.index.task;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Lob;
import javax.persistence.Table;
import javax.persistence.Transient;
import javax.persistence.Version;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.log4j.Logger;
import org.dataone.service.types.v1.SystemMetadata;
import org.dataone.service.util.TypeMarshaller;
import org.jibx.runtime.JiBXException;

@Entity
@Table(name = "index_task")
public class IndexTask implements Serializable {

    @Transient
    private static Logger logger = Logger.getLogger(IndexTask.class.getName());

    /**
     * PK of index_task table
     */
    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    private Long id;

    @Version
    @Column(nullable = false)
    private int version;

    /**
     * The object unique identifier
     */
    @Column(nullable = false)
    private String pid;

    /**
     * The object format id
     */
    private String formatId;

    /**
     * Serialized version of the systemMetaData instance
     */
    @Lob
    @Column
    private String sysMetadata;

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
     * 
     * The lower the priority value, the higher the priority. 0 will be highest
     * priority 99 is no priority?
     **/
    private int priority;
    private static final int PRIORITY_UPDATE = 1;
    private static final int PRIORITY_ADD = 2;
    private static final int PRIORITY_NONE = 99;

    /**
     * An indication that a particular task is being processed, etc. Will
     * indicate "NEW", "IN_PROCESS", "COMPLETE", "FAILED"
     * 
     * The taskModifiedDate is updated when the status flag changes.
     */
    private String status;

    public static final String STATUS_NEW = "NEW";
    public static final String STATUS_IN_PROCESS = "IN PROCESS";
    public static final String STATUS_COMPLETE = "COMPLETE";
    public static final String STATUS_FAILED = "FAILED";

    public IndexTask() {
        this.taskModifiedDate = System.currentTimeMillis();
        this.status = STATUS_NEW;
    }

    public IndexTask(SystemMetadata smd, String objectPath) {
        this();
        if (smd.getIdentifier() != null) {
            this.pid = smd.getIdentifier().getValue();
        }
        if (smd.getFormatId() != null) {
            this.formatId = smd.getFormatId().getValue();
        }
        if (smd.getDateSysMetadataModified() != null) {
            this.dateSysMetaModified = smd.getDateSysMetadataModified().getTime();
        }
        this.marshalSystemMetadata(smd);

        this.setObjectPath(objectPath);

        this.priority = PRIORITY_NONE;
    }

    @Transient
    public SystemMetadata unMarshalSystemMetadata() {
        InputStream is = new ByteArrayInputStream(this.sysMetadata.getBytes());
        SystemMetadata smd = null;
        try {
            smd = TypeMarshaller.unmarshalTypeFromStream(SystemMetadata.class, is);
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        } catch (InstantiationException e) {
            logger.error(e.getMessage(), e);
        } catch (IllegalAccessException e) {
            logger.error(e.getMessage(), e);
        } catch (JiBXException e) {
            logger.error(e.getMessage(), e);
        }
        return smd;
    }

    @Transient
    private void marshalSystemMetadata(SystemMetadata smd) {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        try {
            TypeMarshaller.marshalTypeToOutputStream(smd, os);
        } catch (JiBXException jibxEx) {
            logger.error(jibxEx.getMessage(), jibxEx);
        } catch (IOException ioEx) {
            logger.error(ioEx.getMessage(), ioEx);
        }
        this.sysMetadata = os.toString();
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

    public String getFormatId() {
        return formatId;
    }

    public void setFormatId(String formatid) {
        this.formatId = formatid;
    }

    public String getSysMetadata() {
        return sysMetadata;
    }

    public void setSysMetadata(String sysMetadata) {
        this.sysMetadata = sysMetadata;
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

    @Transient
    public void setUpdatePriority() {
        this.priority = PRIORITY_UPDATE;

    }

    @Transient
    public void setAddPriority() {
        this.priority = PRIORITY_ADD;
    }

    public long getTaskModifiedDate() {
        return taskModifiedDate;
    }

    public void setTaskModifiedDate(long taskModifiedDate) {
        this.taskModifiedDate = taskModifiedDate;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.taskModifiedDate = System.currentTimeMillis();
        this.status = status;
    }

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    @Override
    public String toString() {
        return "IndexTask [id=" + id + ", pid=" + pid + ", formatid=" + formatId + ", objectPath="
                + objectPath + ", dateSysMetaModified=" + dateSysMetaModified
                + ", taskModifiedDate=" + taskModifiedDate + ", priority=" + priority + ", status="
                + status + "]";
    }
}