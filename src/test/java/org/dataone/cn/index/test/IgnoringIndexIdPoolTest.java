package org.dataone.cn.index.test;

import org.dataone.cn.index.task.IgnoringIndexIdPool;
import org.dataone.service.types.v1.Identifier;
import org.dataone.service.types.v2.SystemMetadata;
import org.junit.Assert;
import org.junit.Test;

public class IgnoringIndexIdPoolTest {
    
    @Test
    public void testIsNotIgnorePid () throws Exception {
        SystemMetadata sysm = new SystemMetadata();
        Identifier id = new Identifier();
        id.setValue("foo");
        sysm.setIdentifier(id);
        Assert.assertTrue(IgnoringIndexIdPool.isNotIgnorePid(sysm));
        id.setValue("OBJECT_FORMAT_LIST.1.1");
        sysm.setIdentifier(id);
        Assert.assertFalse(IgnoringIndexIdPool.isNotIgnorePid(sysm));
        id.setValue("OBJECT_FORMAT_LIST.1.2");
        sysm.setIdentifier(id);
        Assert.assertFalse(IgnoringIndexIdPool.isNotIgnorePid(sysm));
    }

}
