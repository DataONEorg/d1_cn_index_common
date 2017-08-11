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
package org.dataone.cn.index.messaging.rabbitmq;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import org.apache.log4j.Logger;
import org.dataone.configuration.Settings;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

/**
 * Represents an object to submit data to the RabbitMQ broker.
 * @author tao
 *
 */
public class MessageSubmitter {
    
    public static final boolean QUEUEDURAL = true;
    public static final boolean QUEUEEXCLUSIVE = false; 
    public static final boolean QUEUEAUTODELETE = false;
    public static final Map<String,Object> QUEUEARGUMENTS = null;
    private static Logger logger = Logger.getLogger(MessageSubmitter.class);
    private Connection connection = null;
    
    /**
     * The default constructor
     * @throws IOException
     * @throws TimeoutException
     */
    public MessageSubmitter() throws IOException, TimeoutException {
        initialConnection();
    }
    
    
    /**
     * Create a connection object. The single connection maybe will be replaced by a connection pool class.
     * @throws IOException
     * @throws TimeoutException
     */
    private void initialConnection() throws IOException, TimeoutException {
        String username = Settings.getConfiguration().getString("messaging.username");
        logger.info("MessageSubmitter.initialConnection - the user name of the connection is "+username);
        String password = Settings.getConfiguration().getString("messaging.password");
        String hostname = Settings.getConfiguration().getString("messaging.hostname");
        logger.info("MessageSubmitter.initialConnection - the host name of the connection is "+hostname);
        ConnectionFactory connFactory = new ConnectionFactory();
        connFactory.setHost(hostname);
        connFactory.setUsername(username);
        connFactory.setPassword(password);
        connection = connFactory.newConnection();
    }
    
    /**
     * Submit a message to the given queue in the broker.
     * @param queueName
     * @param props
     * @param body
     * @return
     * @throws IOException
     */
    public boolean submit(String queueName, BasicProperties props, byte[] body) throws IOException {
        boolean success = false;
        Channel channel = connection.createChannel();
        try {
            channel.confirmSelect();
            //The queue is durable, non-exclusive and non auto-delete.
            channel.queueDeclare(queueName, QUEUEDURAL, QUEUEEXCLUSIVE, QUEUEAUTODELETE, QUEUEARGUMENTS);
            channel.basicPublish("", queueName, props, body);
            logger.info("MessageSubmitter.submit - successfully submit a message to the broker");
            success = true;
        } finally {
            if(channel != null) {
                try {
                    channel.close();
                } catch (Exception e) {
                    logger.warn("MessageSubmitter.submit - in the end of the method, we couldn't close the channel since "+e.getMessage(), e);
                }
                
            }
        }
        return success;
    }
    
    /**
     * A method to close the connection. This method needs to be called in 
     * @throws IOException
     */
    /*public void closeConnection() throws IOException {
        if(connection!= null) {
            logger.info("MessageSubmitter.closeConnection - close the connection ...");
            connection.close();
            logger.info("MessageSubmitter.closeConnection - close the connection sucessfully");
        } else {
            logger.info("MessageSubmitter.closeConnection - the connection wasn't initialized and we don't need to close it at all.");
        }
    }*/
    
    public Connection getConnection() {
        return connection;
    }

    public void setConnection(Connection connection) {
        this.connection = connection;
    }
}
