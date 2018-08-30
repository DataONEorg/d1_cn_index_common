package org.dataone.cn.index.messaging;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

/**
 * This class needs a rabbitMQ server running
 * @author tao
 *
 */
public class IndexTaskMessagingClientFactoryIT {
    
    @Ignore("requires active rabbitMQ broker.  (Underlying MessageSubmitter establishes connection)")
    @Test
    public void testGetClient() throws Exception {
        IndexTaskMessagingClient client = IndexTaskMessagingClientFactory.getClient();
    }
}
