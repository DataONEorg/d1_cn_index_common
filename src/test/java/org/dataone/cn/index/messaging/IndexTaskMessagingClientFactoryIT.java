package org.dataone.cn.index.messaging;

import org.junit.Assert;
import org.junit.Test;

/**
 * This class needs a rabbitMQ server running
 * @author tao
 *
 */
public class IndexTaskMessagingClientFactoryIT {
    @Test
    public void testGetClient() throws Exception {
        IndexTaskMessagingClient client = IndexTaskMessagingClientFactory.getClient();
    }
}
