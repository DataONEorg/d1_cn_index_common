package org.dataone.cn.index.test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.log4j.Logger;
import org.dataone.cn.index.task.IndexTask;
import org.dataone.cn.index.task.IndexTaskRepository;
import org.dataone.service.types.v1.Checksum;
import org.dataone.service.types.v1.Identifier;
import org.dataone.service.types.v1.ObjectFormatIdentifier;
import org.dataone.service.types.v1.Subject;
import org.dataone.service.types.v1.SystemMetadata;
import org.dataone.service.util.TypeMarshaller;
import org.jibx.runtime.JiBXException;
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
    public void testAddOneTask() {
        int initialSize = repo.findAll().size();
        IndexTask task = saveIndexTask("pid1");
        Assert.assertEquals(initialSize + 1, repo.findAll().size());
        IndexTask indexTask = repo.findOne(task.getId());
        Assert.assertEquals(0, "pid1".compareTo(indexTask.getPid()));
    }

    @Test
    public void testUpdateTask() {
        IndexTask it = saveIndexTask("savedPid");
        Long itId = it.getId();
        it.setPid("updatePid");
        repo.save(it);
        it = repo.findOne(itId);
        Assert.assertEquals(0, "updatePid".compareTo(it.getPid()));
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
        repo.save(it);
        it = repo.findOne(it.getId());
        it.setVersion(0);
        it.setUpdatePriority();
        boolean errorFlag = false;
        try {
            // changing the version number should result in a stale object
            // exception from hibernate - optimistic lock failure.
            repo.save(it);
        } catch (HibernateOptimisticLockingFailureException e) {
            logger.info("******* Stale Object Detected!");
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
        Assert.assertEquals(0, pidValue.compareTo(it.getPid()));
    }

    @Test
    public void testFindByPidAndStatusQuery() {
        String pidValue = "find by pid:" + UUID.randomUUID().toString();
        String status = "TEST-STATUS";
        saveIndexTaskWithStatus(pidValue, status);
        List<IndexTask> itList = repo.findByPidAndStatus(pidValue, status);
        Assert.assertEquals(1, itList.size());
        IndexTask it = itList.get(0);
        Assert.assertNotNull(it);
        Assert.assertEquals(0, pidValue.compareTo(it.getPid()));
        Assert.assertEquals(0, status.compareTo(it.getStatus()));
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
        repo.save(task);
        task = repo.findOne(task.getId());
        Assert.assertEquals(0, pidValue.compareTo(task.getPid()));
        Assert.assertEquals(0, formatValue.compareTo(task.getFormatId()));
        smd = task.unMarshalSystemMetadata();
        Assert.assertNotNull(smd);
        Assert.assertEquals(0, pidValue.compareTo(smd.getIdentifier().getValue()));
        Assert.assertEquals(0, formatValue.compareTo(smd.getFormatId().getValue()));
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
            Assert.assertEquals(0, pidValue.compareTo(smd.getIdentifier().getValue()));
            Assert.assertEquals(0, formatValue.compareTo(smd.getFormatId().getValue()));
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        } catch (InstantiationException e) {
            logger.error(e.getMessage(), e);
        } catch (IllegalAccessException e) {
            logger.error(e.getMessage(), e);
        } catch (JiBXException e) {
            logger.error(e.getMessage(), e);
        }
    }

    private IndexTask saveIndexTask(String pid) {
        return saveIndexTaskWithStatus(pid, null);
    }

    private IndexTask saveIndexTaskWithStatus(String pid, String status) {
        IndexTask it = new IndexTask();
        it.setPid(pid);
        it.setStatus(status);
        repo.save(it);
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
