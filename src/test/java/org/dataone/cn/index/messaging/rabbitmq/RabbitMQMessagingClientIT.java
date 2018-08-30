package org.dataone.cn.index.messaging.rabbitmq;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;

import org.dataone.cn.index.task.IndexTask;
import org.dataone.cn.index.task.IndexTaskGenerator;
import org.dataone.service.types.v2.SystemMetadata;
import org.dataone.service.util.TypeMarshaller;
import org.junit.Ignore;
import org.junit.Test;

public class RabbitMQMessagingClientIT {

    /**
     * Test the submit method
     */
    @Ignore("requires a running RabbitMQ broker")
    @Test
    public void submitIT() throws Exception{
        ClassLoader classLoader = getClass().getClassLoader();
        File file = new File(classLoader.getResource("d1_testdocs/peggym.127.1").getFile());
        String objectPath = file.getAbsolutePath();
        System.out.println("The object path is "+objectPath);
        File sysmetaFile = new File(classLoader.getResource("d1_testdocs/peggym.127.1-sysmeta").getFile());
        InputStream is = null;
        SystemMetadata smd = null;
        try {
            is = new FileInputStream(sysmetaFile);
            smd = TypeMarshaller.unmarshalTypeFromStream(SystemMetadata.class, is);
        } catch (Exception e) {
            System.out.println("Can't unmarshal the system metadata file to the system metadata object: "+e.getMessage());
            e.printStackTrace();
        }
        IndexTaskGenerator generator = new IndexTaskGenerator();
        IndexTask task = generator.generateAddTask(smd, objectPath);
        RabbitMQMessagingClient client = new RabbitMQMessagingClient();
        client.submit(task);
    }
}
