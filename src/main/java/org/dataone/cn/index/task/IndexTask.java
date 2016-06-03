/**
 * This work was created by participants in the DataONE project, and is
 * jointly copyrighted by participating institutions in DataONE. For 
 * more information on DataONE, see our web site at http://dataone.org.
 *
 *   Copyright ${year}
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and 
 * limitations under the License.
 * 
 * $Id$
 */

package org.dataone.cn.index.task;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.util.Calendar;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.Transient;
import javax.persistence.Version;

import org.apache.commons.lang.time.FastDateFormat;
import org.apache.log4j.Logger;
import org.dataone.service.types.v2.SystemMetadata;
import org.dataone.service.util.TypeMarshaller;
import org.jibx.runtime.JiBXException;

/**
 * An index task is a POJO that represents an update to a DataONE managed
 * document that needs to be reflected in the search index. An instance of an
 * IndexTask can be processed separately from the components generating the
 * indexTask objects. This requires the IndexTask class to carry enough
 * information that the processing component can properly and efficiently update
 * the search index.
 * 
 * IndexTask is configured via Spring framework as a spring-data JPA object and
 * is stored in relational datastore.
 * 
 * @author sroseboo
 * 
 */
@Entity
@Table(name = "index_task")
public class IndexTask implements Serializable {

    private static final long serialVersionUID = -6319197619205919972L;

    @Transient
    private static Logger logger = Logger.getLogger(IndexTask.class.getName());

    @Transient
    private final FastDateFormat format = FastDateFormat.getInstance("MM/dd/yyyy:HH:mm:ss:SS");

    @Transient
    private static final String FORMAT_RESOURCE_MAP = "http://www.openarchives.org/ore/terms";

    @Transient
    private static final int ALLOWED_RETRIES = 2;

    /**
     * Primary key of index_task table
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
    @Column(columnDefinition = "TEXT", nullable = false)
    private String pid;

    /**
     * The object format id
     */
    private String formatId;

    /**
     * Serialized version of the systemMetaData instance
     */
    @Column(columnDefinition = "TEXT")
    private String sysMetadata;

    /**
     * Filesystem path to the science metadata or resource map object. Null for
     * data objects
     */
    @Column(columnDefinition = "TEXT")
    private String objectPath;

    /**
     * The dateSysMetadataModified of the system metadata
     */
    private long dateSysMetaModified;

    /**
     * task generated/modification date
     */
    private long taskModifiedDate;

    private long nextExecution = 0;

    private int tryCount = 0;

    private boolean deleted = false;

    /**
     * Relative priority of this task. Some operations such as a change in
     * access control rules should be propagated to the index before others
     * 
     * The lower the priority value, the higher the priority. 0 will be highest
     * priority 99 is no priority.
     * 
     * System meta data objects that represent resource maps are given priority
     * within the add, update groups. This way resource maps will be processed
     * at the top of the queue, to short circuit processing index tasks related
     * to the resource map's referenced objects.
     * 
     **/
    private int priority;
    private static final int PRIORITY_UPDATE = 1;
    private static final int PRIORITY_ADD = 2;
    private static final int PRIOIRTY_UPDATE_RESOURCE_MAP = 3;
    private static final int PRIORITY_ADD_RESOURCE_MAP = 4;
    private static final int PRIORITY_NONE = 99;

    /**
     * An indication that a particular task is being processed, etc. Will
     * indicate "NEW", "IN_PROCESS", "COMPLETE", "FAILED"
     * 
     * The taskModifiedDate is updated when the status flag changes.
     */
    private String status;

    /**
     * IndexTask processing status of new, unprocessed.
     */
    public static final String STATUS_NEW = "NEW";
    /**
     * IndexTask processing status to represent task currently being processed.
     */
    public static final String STATUS_IN_PROCESS = "IN PROCESS";
    /**
     * IndexTask processing status to represent a task that has successfully
     * completed.
     */
    public static final String STATUS_COMPLETE = "COMPLETE";
    /**
     * IndexTask processing status to represent a task that has failed
     * processing.
     */
    public static final String STATUS_FAILED = "FAILED";

    public IndexTask() {
        this.taskModifiedDate = System.currentTimeMillis();
        this.status = STATUS_NEW;
    }

    /**
     * Construct an IndexTask for the given SystemMetadata and objectPath
     * information.
     * 
     * @param smd
     * @param objectPath
     */
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
        try {
            this.sysMetadata = os.toString("UTF-8");
        } catch (UnsupportedEncodingException e) {
            logger.error(e.getMessage());
        }
    }

    /**
     * Does this task represent an update for an archived document.
     * 
     * @return
     */
    @Transient
    private boolean isArchived() {
        boolean archived = false;
        SystemMetadata smd = unMarshalSystemMetadata();
        if (smd.getArchived() != null && smd.getArchived().booleanValue()) {
            archived = true;
        }
        return archived;
    }

    /**
     * Does this task represent a removal from the search index.
     * 
     * @return
     */
    @Transient
    public boolean isDeleteTask() {
        return isDeleted() || isArchived();
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

    public boolean isDeleted() {
        return deleted;
    }

    public void setDeleted(boolean deleted) {
        this.deleted = deleted;
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

    public long getNextExecution() {
        return this.nextExecution;
    }

    public void setNextExection(long next) {
        this.nextExecution = next;
    }

    public int getTryCount() {
        return tryCount;
    }

    public void setTryCount(int count) {
        this.tryCount = count;
    }

    /**
     * Private method exposed due to JPA and unit testing requirements. Should
     * not use directly.
     * 
     * @return
     */
    public int getPriority() {
        return priority;
    }

    /**
     * Private method exposed due to JPA and unit testing requirements. Should
     * not use directly. See setUpdatePriority, setAddPriority methods.
     * 
     * @return
     */
    public void setPriority(int priority) {
        this.priority = priority;
    }

    /**
     * Assign update priority to this IndexTask. Priority is used by processing
     * to determine what order to process IndexTasks.
     */
    @Transient
    public void setUpdatePriority() {
        if (isResourceMap()) {
            this.priority = PRIOIRTY_UPDATE_RESOURCE_MAP;
        } else {
            this.priority = PRIORITY_UPDATE;
        }
    }

    /**
     * Assign add priority to this IndexTask. Priority is used by processing to
     * determine what order to process IndexTasks.
     */
    @Transient
    public void setAddPriority() {
        if (isResourceMap()) {
            this.priority = PRIORITY_ADD_RESOURCE_MAP;
        } else {
            this.priority = PRIORITY_ADD;
        }
    }

    @Transient
    private boolean isResourceMap() {
        return FORMAT_RESOURCE_MAP.equals(this.formatId);
    }

    public long getTaskModifiedDate() {
        return taskModifiedDate;
    }

    public void setTaskModifiedDate(long taskModifiedDate) {
        this.taskModifiedDate = taskModifiedDate;
    }

    @Transient
    public String getTaskModDateString() {
        return format.format(this.getTaskModifiedDate());
    }

    public String getStatus() {
        return status;
    }

    /**
     * Do not use this method, used by unit tests only.
     * use the specific 'markNew, markFailed, markInProcess' methods.
     */
    public void setStatus(String status) {
        if (status != null) {
            this.taskModifiedDate = System.currentTimeMillis();
            this.status = status;
        }
    }

    private void setBackoffExectionTime() {
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(System.currentTimeMillis());
        int tryCount = getTryCount();
        if (tryCount == ALLOWED_RETRIES) {
            cal.add(Calendar.MINUTE, 20);
            setNextExection(cal.getTimeInMillis());
        } else if (tryCount == ALLOWED_RETRIES + 1) {
            cal.add(Calendar.HOUR, 2);
            setNextExection(cal.getTimeInMillis());
        } else if (tryCount == ALLOWED_RETRIES + 2) {
            cal.add(Calendar.HOUR, 8);
            setNextExection(cal.getTimeInMillis());
        } else if (tryCount >= ALLOWED_RETRIES + 3 && tryCount <= ALLOWED_RETRIES + 5) {
            cal.add(Calendar.HOUR, 24);
            setNextExection(cal.getTimeInMillis());
        } else if (tryCount > ALLOWED_RETRIES + 5) {
            cal.add(Calendar.DATE, 7);
            setNextExection(cal.getTimeInMillis());
        }
    }

    private boolean timeForRetryBackoff(String status) {
        return (getTryCount() >= ALLOWED_RETRIES) && (STATUS_COMPLETE.equals(status) == false)
                && (STATUS_IN_PROCESS.equals(status) == false);
    }

    public void markInProgress() {
        this.setStatus(STATUS_IN_PROCESS);
        this.tryCount++;
    }

    public void markNew() {
        this.setStatus(STATUS_NEW);
        if (timeForRetryBackoff(status)) {
            logger.info("Even tough it was masked new, it is still considered failed for id "+pid+" since it was tried to many times.");
            this.status = STATUS_FAILED;
            setBackoffExectionTime();
        }
    }

    public void markFailed() {
        this.setStatus(STATUS_FAILED);
        if (timeForRetryBackoff(status)) {
            this.status = STATUS_FAILED;
            setBackoffExectionTime();
        }
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
                + objectPath + ", dateSysMetaModified=" + dateSysMetaModified + ", deleted="
                + deleted + ", taskModifiedDate=" + taskModifiedDate + ", priority=" + priority
                + ", status=" + status + "]";
    }

}
