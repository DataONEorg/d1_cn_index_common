package org.dataone.cn.index.test;

import java.util.List;

import org.dataone.cn.index.task.IndexTask;
import org.dataone.cn.index.task.IndexTaskRepository;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
// context files are located from the root of the test's classpath
// for example org/dataone/cn/index/test/
@ContextConfiguration(locations = { "test-context.xml" })
public class IndexTaskJpaRepositoryTest {

    @Autowired
    private IndexTaskRepository repo;

    @Test
    public void testRepoInjection() {
        Assert.assertNotNull(repo);
    }

    @Test
    public void testAddOneTask() {
        saveIndexTask("pid1");
        List<IndexTask> results = repo.findAll();
        Assert.assertEquals(1, results.size());
        IndexTask indexTask = results.get(0);
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
        IndexTask it = saveIndexTask("deleteThis");
        Long itId = it.getId();
        repo.delete(it);
        Assert.assertNull(repo.findOne(itId));
        Assert.assertFalse(repo.exists(itId));
    }

    private IndexTask saveIndexTask(String pid) {
        IndexTask it = new IndexTask();
        it.setPid(pid);
        repo.save(it);
        return it;
    }
}
