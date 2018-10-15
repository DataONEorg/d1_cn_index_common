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

package org.dataone.cn.index.test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.dataone.exceptions.MarshallingException;
import org.apache.log4j.Logger;
import org.dataone.cn.index.task.IndexTask;
import org.dataone.cn.index.task.IndexTaskRepository;
import org.dataone.service.types.v1.Checksum;
import org.dataone.service.types.v1.Identifier;
import org.dataone.service.types.v1.ObjectFormatIdentifier;
import org.dataone.service.types.v1.Subject;
import org.dataone.service.types.v2.SystemMetadata;
import org.dataone.service.util.TypeMarshaller;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.orm.hibernate3.HibernateOptimisticLockingFailureException;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
// context files are located from the root of the test's classpath
// for example org/dataone/cn/index/test/
@ContextConfiguration(locations = { "test-context.xml" })
public class IndexTaskJpaRepositoryTest {

    private static Logger logger = Logger.getLogger(IndexTaskJpaRepositoryTest.class.getName());

    @Autowired
    private IndexTaskRepository repo;

    @Test
    public void testRepositoryInjection() {
        Assert.assertNotNull(repo);
    }

    @Test
    public void testTaskExecutionBackoffForRetry() {
        repo.deleteAll();
        // noise
        saveIndexTaskWithStatus(UUID.randomUUID().toString(), IndexTask.STATUS_NEW);
        String pidValue = "find by pid:" + UUID.randomUUID().toString();
        IndexTask task = saveIndexTaskWithStatus(pidValue, IndexTask.STATUS_NEW);

        task = simulateMarkNewProcessing(task);
        List<IndexTask> itList = repo
                .findByStatusOrderByPriorityAscTaskModifiedDateAsc(IndexTask.STATUS_NEW);
        Assert.assertEquals(2, itList.size());

        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(System.currentTimeMillis());
        cal.add(Calendar.MONTH, 1);
        itList = repo.findByStatusAndNextExecutionLessThan(IndexTask.STATUS_FAILED,
                cal.getTimeInMillis());
        Assert.assertEquals(0, itList.size());

        Calendar cal1 = Calendar.getInstance();
        cal1.add(Calendar.MINUTE, 18);
        Calendar cal2 = Calendar.getInstance();
        cal2.add(Calendar.MINUTE, 22);
        task = testNextBackoffForRetry(task, cal1, cal2);

        cal1.setTimeInMillis(System.currentTimeMillis());
        cal1.add(Calendar.MINUTE, 22);
        cal2.setTimeInMillis(System.currentTimeMillis());
        cal2.add(Calendar.MINUTE, 122);
        task = testNextBackoffForRetry(task, cal1, cal2);

        cal1.setTimeInMillis(System.currentTimeMillis());
        cal1.add(Calendar.MINUTE, 122);
        cal2.setTimeInMillis(System.currentTimeMillis());
        cal2.add(Calendar.MINUTE, (60 * 8) + 2);
        task = testNextBackoffForRetry(task, cal1, cal2);

        cal1.setTimeInMillis(System.currentTimeMillis());
        cal1.add(Calendar.MINUTE, (60 * 8) + 2);
        cal2.setTimeInMillis(System.currentTimeMillis());
        cal2.add(Calendar.MINUTE, (60 * 24) + 2);
        task = testNextBackoffForRetry(task, cal1, cal2);
        task = testNextBackoffForRetry(task, cal1, cal2);
        task = testNextBackoffForRetry(task, cal1, cal2);

        cal1.setTimeInMillis(System.currentTimeMillis());
        cal1.add(Calendar.MINUTE, (60 * 24) + 2);
        cal2.setTimeInMillis(System.currentTimeMillis());
        cal2.add(Calendar.DATE, 7);
        cal2.add(Calendar.MINUTE, 2);
        task = testNextBackoffForRetry(task, cal1, cal2);
        task = testNextBackoffForRetry(task, cal1, cal2);
    }

    @Test
    public void testTaskExecutionBackoffForFailed() {
        repo.deleteAll();
        // noise
        saveIndexTaskWithStatus(UUID.randomUUID().toString(), IndexTask.STATUS_NEW);
        String pidValue = "find by pid:" + UUID.randomUUID().toString();
        IndexTask task = saveIndexTaskWithStatus(pidValue, IndexTask.STATUS_NEW);

        task = simulateMarkFailedProcessing(task);
        List<IndexTask> itList = repo
                .findByStatusOrderByPriorityAscTaskModifiedDateAsc(IndexTask.STATUS_NEW);
        Assert.assertEquals(1, itList.size());

        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(System.currentTimeMillis());
        cal.add(Calendar.MINUTE, 1);
        itList = repo.findByStatusAndNextExecutionLessThan(IndexTask.STATUS_FAILED,
                cal.getTimeInMillis());
        Assert.assertEquals(1, itList.size());

        Calendar cal1 = Calendar.getInstance();
        cal1.add(Calendar.MINUTE, 18);
        Calendar cal2 = Calendar.getInstance();
        cal2.add(Calendar.MINUTE, 22);
        task = testNextBackoffForFailed(task, cal1, cal2);

        cal1.setTimeInMillis(System.currentTimeMillis());
        cal1.add(Calendar.MINUTE, 22);
        cal2.setTimeInMillis(System.currentTimeMillis());
        cal2.add(Calendar.MINUTE, 122);
        task = testNextBackoffForFailed(task, cal1, cal2);

        cal1.setTimeInMillis(System.currentTimeMillis());
        cal1.add(Calendar.MINUTE, 122);
        cal2.setTimeInMillis(System.currentTimeMillis());
        cal2.add(Calendar.MINUTE, (60 * 8) + 2);
        task = testNextBackoffForFailed(task, cal1, cal2);

        cal1.setTimeInMillis(System.currentTimeMillis());
        cal1.add(Calendar.MINUTE, (60 * 8) + 2);
        cal2.setTimeInMillis(System.currentTimeMillis());
        cal2.add(Calendar.MINUTE, (60 * 24) + 2);
        task = testNextBackoffForFailed(task, cal1, cal2);
        task = testNextBackoffForFailed(task, cal1, cal2);
        task = testNextBackoffForFailed(task, cal1, cal2);

        cal1.setTimeInMillis(System.currentTimeMillis());
        cal1.add(Calendar.MINUTE, (60 * 24) + 2);
        cal2.setTimeInMillis(System.currentTimeMillis());
        cal2.add(Calendar.DATE, 7);
        cal2.add(Calendar.MINUTE, 2);
        task = testNextBackoffForFailed(task, cal1, cal2);
        task = testNextBackoffForFailed(task, cal1, cal2);
    }

    private IndexTask testNextBackoffForRetry(IndexTask task, Calendar previousTimeIncrement,
            Calendar nextTimeIncrement) {

        task = simulateMarkNewProcessing(task);
        List<IndexTask> itList = repo.findByStatusAndNextExecutionLessThan(IndexTask.STATUS_FAILED,
                previousTimeIncrement.getTimeInMillis());
        Assert.assertEquals(0, itList.size());

        itList = repo.findByStatusAndNextExecutionLessThan(IndexTask.STATUS_FAILED,
                nextTimeIncrement.getTimeInMillis());
        Assert.assertEquals(1, itList.size());

        IndexTask it = itList.get(0);
        Assert.assertNotNull(it);
        Assert.assertTrue(task.getPid().equals(it.getPid()));
        Assert.assertTrue(IndexTask.STATUS_FAILED.equals(it.getStatus()));
        return task;
    }

    private IndexTask testNextBackoffForFailed(IndexTask task, Calendar previousTimeIncrement,
            Calendar nextTimeIncrement) {

        task = simulateMarkFailedProcessing(task);
        List<IndexTask> itList = repo.findByStatusAndNextExecutionLessThan(IndexTask.STATUS_FAILED,
                previousTimeIncrement.getTimeInMillis());
        Assert.assertEquals(0, itList.size());

        itList = repo.findByStatusAndNextExecutionLessThan(IndexTask.STATUS_FAILED,
                nextTimeIncrement.getTimeInMillis());
        Assert.assertEquals(1, itList.size());

        IndexTask it = itList.get(0);
        Assert.assertNotNull(it);
        Assert.assertTrue(task.getPid().equals(it.getPid()));
        Assert.assertTrue(IndexTask.STATUS_FAILED.equals(it.getStatus()));
        return task;
    }

    private IndexTask simulateMarkNewProcessing(IndexTask task) {
        task.markInProgress();
        // not ready
        task.markNew();
        task = repo.save(task);
        return task;
    }

    private IndexTask simulateMarkFailedProcessing(IndexTask task) {
        task.markInProgress();
        // not ready
        task.markFailed();
        task = repo.save(task);
        return task;
    }

    @Test
    public void testAddOneTask() {
        int initialSize = repo.findAll().size();
        IndexTask task = saveIndexTask("pid1");
        Assert.assertEquals(initialSize + 1, repo.findAll().size());
        IndexTask indexTask = repo.findOne(task.getId());
        Assert.assertTrue("pid1".equals(indexTask.getPid()));
    }

    @Test
    public void testUpdateTask() {
        IndexTask it = saveIndexTask("savedPid");
        Long itId = it.getId();
        it.setPid("updatePid");
        it = repo.save(it);
        it = repo.findOne(itId);
        Assert.assertTrue("updatePid".equals(it.getPid()));
    }

    @Test
    public void testDeleteTask() {
        int intialSize = repo.findAll().size();
        IndexTask it = saveIndexTask("deleteThis");
        Long itId = it.getId();
        repo.delete(it);
        Assert.assertNull(repo.findOne(itId));
        Assert.assertFalse(repo.exists(itId));
        Assert.assertEquals(intialSize, repo.findAll().size());
    }

    @Test
    public void testVersioning() {
        IndexTask it = saveIndexTask("version-test");
        it.setAddPriority();
        it = repo.save(it);
        it = repo.findOne(it.getId());
        it.setVersion(0);
        it.setUpdatePriority();
        boolean errorFlag = false;
        try {
            // changing the version number should result in a stale object
            // exception from hibernate - optimistic lock failure.
            it = repo.save(it);
        } catch (HibernateOptimisticLockingFailureException e) {
            logger.info("******* Stale Object Detected (as expected)!");
            errorFlag = true;
        }
        Assert.assertTrue(errorFlag);
    }

    @Test
    public void testFindByPidQuery() {
        String pidValue = "find by pid:" + UUID.randomUUID().toString();
        saveIndexTask(pidValue);
        List<IndexTask> itList = repo.findByPid(pidValue);
        Assert.assertEquals(1, itList.size());
        IndexTask it = itList.get(0);
        Assert.assertNotNull(it);
        Assert.assertTrue(pidValue.equals(it.getPid()));
    }

    @Test
    public void testFindByPidAndStatusQuery() {
        String pidValue = "find by pid:" + UUID.randomUUID().toString();
        String status = "TEST-STATUS";
        saveIndexTaskWithStatus(pidValue, status);

        String pidValue2 = "find by pid:" + UUID.randomUUID().toString();
        String status2 = "TEST-STATUS2";
        saveIndexTaskWithStatus(pidValue2, status2);

        List<IndexTask> itList = repo.findByPidAndStatus(pidValue, status);
        Assert.assertEquals(1, itList.size());
        IndexTask it = itList.get(0);
        Assert.assertNotNull(it);
        Assert.assertTrue(pidValue.equals(it.getPid()));
        Assert.assertTrue(status.equals(it.getStatus()));
    }

    @Test
    public void testFindByStatusAndNextExection() {
        String pidValue = "find by pid:" + UUID.randomUUID().toString();
        String status = "TEST-NEXT";
        saveIndexTaskWithStatus(pidValue, status);

        String pidValue2 = "find by pid:" + UUID.randomUUID().toString();
        IndexTask task2 = saveIndexTaskWithStatus(pidValue2, "TEST-NEXT");
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(System.currentTimeMillis());
        cal.add(Calendar.DATE, 1);
        task2.setNextExection(cal.getTimeInMillis());
        task2 = repo.save(task2);

        List<IndexTask> itList = repo.findByStatusAndNextExecutionLessThan(status,
                System.currentTimeMillis());
        Assert.assertEquals(1, itList.size());
        IndexTask it = itList.get(0);
        Assert.assertNotNull(it);
        Assert.assertTrue(pidValue.equals(it.getPid()));
        Assert.assertTrue(status.equals(it.getStatus()));
    }

    /**
     * Tests status narrowing and ordering of the task queue query
     */
    @Test
    public void testFindIndexTaskQueue() {
        saveIndexTaskWithStatusAndPriority("garbage task" + UUID.randomUUID().toString(),
                "garbage status", 1);

        String status = "findQueue";
        String status2 = "badStatus";

        repo.deleteAll();

        // created first, should be first among priority 2 items
        String pidValue1 = "1st created task: " + UUID.randomUUID().toString();
        saveIndexTaskWithStatusAndPriority(pidValue1, status, 2);

        // created second, should be second among priority 2 items
        String pidValue2 = "2nd created task: " + UUID.randomUUID().toString();
        saveIndexTaskWithStatusAndPriority(pidValue2, status, 2);

        // created last, but should be first due to highest priority
        String pidValue3 = "3rd created task: " + UUID.randomUUID().toString();
        saveIndexTaskWithStatusAndPriority(pidValue3, status, 1);

        String pidValue4 = "4th created task: " + UUID.randomUUID().toString();
        saveIndexTaskWithStatusAndPriority(pidValue4, status2, 1);

        String pidValue5 = "thrd created task: " + UUID.randomUUID().toString();
        saveIndexTaskWithStatusAndPriority(pidValue5, status2, 1);

        List<IndexTask> queue = repo.findByStatusOrderByPriorityAscTaskModifiedDateAsc(status);
        Assert.assertEquals(3, queue.size());

        IndexTask task = queue.get(0);
        logger.info("First queue task: " + task.getPid() + " priority: " + task.getPriority()
                + " task build time: " + task.getTaskModDateString());

        IndexTask task2 = queue.get(1);
        logger.info("Second queue task: " + task2.getPid() + " priority: " + task2.getPriority()
                + " task build time: " + task2.getTaskModDateString());

        IndexTask task3 = queue.get(2);
        logger.info("Second queue task: " + task3.getPid() + " priority: " + task3.getPriority()
                + " task build time: " + task3.getTaskModDateString());

        Assert.assertTrue(pidValue3.equals(task.getPid()));
        Assert.assertTrue(pidValue1.equals(task2.getPid()));
        Assert.assertTrue(pidValue2.equals(task3.getPid()));
    }
    
    /**
     * Test the method of findByStatusOrderAndTryCount
     */
    @Test
    public void testFindByStatusOrderAndTryCount() {
        String status = "new";
        String status2 = "new2";
      
        repo.deleteAll();

        // created first, should be first among priority 2 items
        String pidValue1 = "1st created task: " + UUID.randomUUID().toString();
        saveIndexTaskWithStatusAndPriorityAndTryCount(pidValue1, status, 2, 1);

        // created second, should be second among priority 2 items
        String pidValue2 = "2nd created task: " + UUID.randomUUID().toString();
        saveIndexTaskWithStatusAndPriorityAndTryCount(pidValue2, status2, 2, 1);

        // created last, but should be first due to highest priority
        String pidValue3 = "3rd created task: " + UUID.randomUUID().toString();
        saveIndexTaskWithStatusAndPriorityAndTryCount(pidValue3, status, 1, 5);

        String pidValue4 = "4th created task: " + UUID.randomUUID().toString();
        saveIndexTaskWithStatusAndPriorityAndTryCount(pidValue4, status, 1, 4);

        String pidValue5 = "thrd created task: " + UUID.randomUUID().toString();
        saveIndexTaskWithStatusAndPriorityAndTryCount(pidValue5, status, 1, 3);
        
        List<IndexTask> queue = repo.findByStatusAndTryCountLessThanOrderByPriorityAscTaskModifiedDateAsc(status, 5);
        Assert.assertEquals(3, queue.size());
        
        queue = repo.findByStatusAndTryCountLessThanOrderByPriorityAscTaskModifiedDateAsc(status2, 5);
        Assert.assertEquals(1, queue.size());
        Assert.assertTrue(pidValue2.equals(queue.get(0).getPid()));

        queue = repo.findByStatusAndTryCountLessThanOrderByPriorityAscTaskModifiedDateAsc(status, 4);
        Assert.assertEquals(2, queue.size());
        Assert.assertTrue(pidValue5.equals(queue.get(0).getPid()));
        Assert.assertTrue(pidValue1.equals(queue.get(1).getPid()));
        
        queue = repo.findByStatusAndTryCountLessThanOrderByPriorityAscTaskModifiedDateAsc(status, 3);
        Assert.assertEquals(1, queue.size());
        Assert.assertTrue(pidValue1.equals(queue.get(0).getPid()));
        
    }
    
    /**
     * Test the method of findByStatusAndNextExecutionLessThanAndTryCountLessThan
     */
    @Test
    public void testFindByStatusAndNextExecutionLessThanAndTryCountLessThan() throws Exception {
        String status = "new";
        String status2 = "new2";
      
        repo.deleteAll();

        long firstPoint = System.currentTimeMillis();
        // created first, should be first among priority 2 items
        String pidValue1 = "1st created task: " + UUID.randomUUID().toString();
        saveIndexTaskWithStatusAndPriorityAndTryCountAndNextExecution(pidValue1, status, 2, 1, firstPoint);

        // created second, should be second among priority 2 items
        String pidValue2 = "2nd created task: " + UUID.randomUUID().toString();
        saveIndexTaskWithStatusAndPriorityAndTryCountAndNextExecution(pidValue2, status2, 2, 1, firstPoint);

        Thread.sleep(100);
        long secondPoint = System.currentTimeMillis();
        Thread.sleep(100);
        long thirdPoint = System.currentTimeMillis();
        Thread.sleep(100);
        // created last, but should be first due to highest priority
        String pidValue3 = "3rd created task: " + UUID.randomUUID().toString();
        saveIndexTaskWithStatusAndPriorityAndTryCountAndNextExecution(pidValue3, status, 1, 5, thirdPoint);

        
        String pidValue4 = "4th created task: " + UUID.randomUUID().toString();
        saveIndexTaskWithStatusAndPriorityAndTryCountAndNextExecution(pidValue4, status, 1, 4, thirdPoint);


        String pidValue5 = "thrd created task: " + UUID.randomUUID().toString();
        saveIndexTaskWithStatusAndPriorityAndTryCountAndNextExecution(pidValue5, status, 1, 3, thirdPoint);
        
        List<IndexTask> queue = repo.findByStatusAndNextExecutionLessThanAndTryCountLessThan(status, System.currentTimeMillis(), 5);
        Assert.assertEquals(3, queue.size());
        
        queue = repo.findByStatusAndNextExecutionLessThanAndTryCountLessThan(status, secondPoint, 5);
        Assert.assertEquals(1, queue.size());
        Assert.assertTrue(pidValue1.equals(queue.get(0).getPid()));
        
        queue = repo.findByStatusAndNextExecutionLessThanAndTryCountLessThan(status, secondPoint, 3);
        Assert.assertEquals(1, queue.size());
        Assert.assertTrue(pidValue1.equals(queue.get(0).getPid()));
        
        queue = repo.findByStatusAndNextExecutionLessThanAndTryCountLessThan(status2, System.currentTimeMillis(), 5);
        Assert.assertEquals(1, queue.size());
        Assert.assertTrue(pidValue2.equals(queue.get(0).getPid()));

        queue = repo.findByStatusAndNextExecutionLessThanAndTryCountLessThan(status, System.currentTimeMillis(), 4);
        Assert.assertEquals(2, queue.size());
        //Assert.assertTrue(pidValue1.equals(queue.get(0).getPid()));
        //Assert.assertTrue(pidValue5.equals(queue.get(1).getPid()));
        
        queue = repo.findByStatusAndNextExecutionLessThanAndTryCountLessThan(status, System.currentTimeMillis(), 3);
        Assert.assertEquals(1, queue.size());
        Assert.assertTrue(pidValue1.equals(queue.get(0).getPid()));
    }

    /**
     * This test creates a 'test' system meta data instance and uses the task
     * generator to create an index task. After the IndexTask is stored are
     * retrieved, the instance of systemMetadata is un-marshaled from the
     * IndexTask to test marshal/un-marshal logic of IndexTask.
     * 
     */
    @Test
    public void testTaskSystemMetadataMarshaling() {
        String pidValue = "gent-test-AddTask-" + UUID.randomUUID().toString();
        String formatValue = "CF-1.0";
        SystemMetadata smd = buildTestSysMetaData(pidValue, formatValue);
        IndexTask task = new IndexTask(smd, null);
        task = repo.save(task);
        task = repo.findOne(task.getId());
        Assert.assertTrue(pidValue.equals(task.getPid()));
        Assert.assertTrue(formatValue.equals(task.getFormatId()));
        smd = task.unMarshalSystemMetadata();
        Assert.assertNotNull(smd);
        Assert.assertTrue(pidValue.equals(smd.getIdentifier().getValue()));
        Assert.assertTrue(formatValue.equals(smd.getFormatId().getValue()));
    }

    /**
     * This test is a redundant test of serializing, de-serializing system
     * metadata object. Sanity-check.
     */
    public void testStream() {
        try {
            String pidValue = "marshal-test" + UUID.randomUUID().toString();
            String formatValue = "marshal-format";
            SystemMetadata smd = buildTestSysMetaData(pidValue, formatValue);
            ByteArrayOutputStream os = new ByteArrayOutputStream();
            TypeMarshaller.marshalTypeToOutputStream(smd, os);
            InputStream is = new ByteArrayInputStream(os.toByteArray());
            SystemMetadata smdNew = TypeMarshaller
                    .unmarshalTypeFromStream(SystemMetadata.class, is);
            Assert.assertNotNull(smd);
            Assert.assertTrue(pidValue.equals(smd.getIdentifier().getValue()));
            Assert.assertTrue(formatValue.equals(smd.getFormatId().getValue()));
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        } catch (InstantiationException e) {
            logger.error(e.getMessage(), e);
        } catch (IllegalAccessException e) {
            logger.error(e.getMessage(), e);
        } catch (MarshallingException e) {
            logger.error(e.getMessage(), e);
        }
    }

    private IndexTask saveIndexTask(String pid) {
        return saveIndexTaskWithStatus(pid, null);
    }

    private IndexTask saveIndexTaskWithStatus(String pid, String status) {
        return saveIndexTaskWithStatusAndPriority(pid, status, 99);
    }

    private IndexTask saveIndexTaskWithStatusAndPriority(String pid, String status, int priority) {
        IndexTask it = new IndexTask(buildTestSysMetaData(pid, "test-format"), "test object path");
        it.setPid(pid);
        it.setStatus(status);
        it.setPriority(priority);
        it = repo.save(it);
        return it;
    }
    
    private IndexTask saveIndexTaskWithStatusAndPriorityAndTryCount(String pid, String status, int priority, int tryCount) {
        IndexTask it = new IndexTask(buildTestSysMetaData(pid, "test-format"), "test object path");
        it.setPid(pid);
        it.setStatus(status);
        it.setPriority(priority);
        it.setTryCount(tryCount);
        it = repo.save(it);
        return it;
    }
    
    private IndexTask saveIndexTaskWithStatusAndPriorityAndTryCountAndNextExecution(String pid, String status, int priority, int tryCount, long nextExec) {
        IndexTask it = new IndexTask(buildTestSysMetaData(pid, "test-format"), "test object path");
        it.setPid(pid);
        it.setStatus(status);
        it.setPriority(priority);
        it.setTryCount(tryCount);
        it.setNextExection(nextExec);
        it = repo.save(it);
        return it;
    }


    public SystemMetadata buildTestSysMetaData(String pidValue, String formatValue) {
        SystemMetadata systemMetadata = new SystemMetadata();

        Identifier identifier = new Identifier();
        identifier.setValue(pidValue);
        systemMetadata.setIdentifier(identifier);

        ObjectFormatIdentifier fmtid = new ObjectFormatIdentifier();
        fmtid.setValue(formatValue);
        systemMetadata.setFormatId(fmtid);

        systemMetadata.setSerialVersion(BigInteger.TEN);
        systemMetadata.setSize(BigInteger.TEN);
        Checksum checksum = new Checksum();
        checksum.setValue("V29ybGQgSGVsbG8h");
        checksum.setAlgorithm("SHA-1");
        systemMetadata.setChecksum(checksum);

        Subject rightsHolder = new Subject();
        rightsHolder.setValue("DataONE");
        systemMetadata.setRightsHolder(rightsHolder);

        Subject submitter = new Subject();
        submitter.setValue("Kermit de Frog");
        systemMetadata.setSubmitter(submitter);

        systemMetadata.setDateSysMetadataModified(new Date());
        return systemMetadata;
    }
}
