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
package org.dataone.cn.index.messaging.rabbitmq.connectionpool;

import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.log4j.Logger;
import org.dataone.cn.index.messaging.rabbitmq.MessageSubmitter;
import org.dataone.configuration.Settings;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;


/**
 * An object represents a factory to manage the life cycle of pooled objects
 * @author tao
 *
 */
public class PooledConnectionFactory extends BasePooledObjectFactory<Connection> {
    private static Logger logger = Logger.getLogger(MessageSubmitter.class);
    private ConnectionFactory connFactory = null;
    
    /**
     * Default Constructor
     */
    public PooledConnectionFactory() {
        String username = Settings.getConfiguration().getString("messaging.username");
        logger.info("PooledConnectionFactory.constructor - the user name of the connection is "+username);
        String password = Settings.getConfiguration().getString("messaging.password");
        String hostname = Settings.getConfiguration().getString("messaging.hostname");
        logger.info("PooledConnectionFactory.constructor - the host name of the connection is "+hostname);
        connFactory = new ConnectionFactory();
        connFactory.setHost(hostname);
        connFactory.setUsername(username);
        connFactory.setPassword(password);
    }
    
    @Override
    public Connection create() throws Exception {
        Connection connection = connFactory.newConnection();
        return connection;
    }
    
    @Override
    public PooledObject<Connection> wrap(Connection connection) {
        return new DefaultPooledObject<Connection>(connection);
    }
    
    @Override
    public boolean validateObject(PooledObject<Connection> connection) {
        return connection.getObject().isOpen();
    }
    
    @Override
    public void destroyObject(PooledObject<Connection> connection) throws Exception {
        connection.getObject().close();
    }

}
