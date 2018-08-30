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
import java.util.Map.Entry;
import java.util.concurrent.TimeoutException;

import org.apache.log4j.Logger;
import org.dataone.configuration.Settings;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.AMQP.BasicProperties.Builder;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConfirmListener;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

/**
 * Represents an object to submit data to the RabbitMQ broker.
 * @author tao
 *
 */
public class MessageSubmitter {
    
    public static final boolean QUEUEDURABLE = true;
    public static final boolean QUEUEEXCLUSIVE = false; 
    public static final boolean QUEUEAUTODELETE = false;
    public static final Map<String,Object> QUEUEARGUMENTS = null;
    
    private static Logger logger = Logger.getLogger(MessageSubmitter.class);
    private Connection connection = null;
    private Channel confirmModeChannel = null;
    
    /**
     * The default constructor
     * @throws IOException
     * @throws TimeoutException
     */
    public MessageSubmitter() throws IOException, TimeoutException {
        initializeConnection();
        initializeChannel();
    }
    

    /**
     * Alternate constructor 
     * @param connection
     * @throws IOException
     * @throws TimeoutException
     */
    public MessageSubmitter(Connection connection) throws IOException, TimeoutException {
        //initialConnection();
        this.connection = connection;
    }
    
    
    /**
     * Create a connection object. The single connection maybe will be replaced by a connection pool class.
     * @throws IOException
     * @throws TimeoutException
     */
    private void initializeConnection() throws IOException, TimeoutException {
        String username = Settings.getConfiguration().getString("messaging.username");
        logger.info("MessageSubmitter.initialConnection - the user name of the connection is "+username);
        String password = Settings.getConfiguration().getString("messaging.password",null);
        if (password == null || password.equals(""))
                logger.warn("MessageSubmitter.initialConnection - the password for the connection is null or empty!");
        String hostname = Settings.getConfiguration().getString("messaging.hostname");
        logger.info("MessageSubmitter.initialConnection - the host name of the connection is "+hostname);
        ConnectionFactory connFactory = new ConnectionFactory();
        connFactory.setHost(hostname);
        connFactory.setUsername(username);
        connFactory.setPassword(password);
        connection = connFactory.newConnection();
    }

       
    private void initializeChannel() throws IOException {
        confirmModeChannel = connection.createChannel();
        confirmModeChannel.confirmSelect();
        
        confirmModeChannel.addConfirmListener(new ConfirmListener() {

            /**
             * @param acknowledgeMultiple: if true, Acks everything up to this deliveryTag since last ack or nack
             */

            @Override
            public void handleAck(long deliveryTag, boolean acknowledgeMultiple) throws IOException {
                if (acknowledgeMultiple)
                    logger.info(String.format("Messages up to %d confirmed with ACK", deliveryTag));
                else {
                    logger.info(String.format("Message %d confirmed with ACK", deliveryTag));
                }
                
            }

            /**
             * @param acknowledgeMultiple: if true, Nacks everything up to this deliveryTag since last ack or nack
             */
            @Override
            public void handleNack(long deliveryTag, boolean acknowledgeMultiple) throws IOException {
                
                if (acknowledgeMultiple)
                    logger.info(String.format("Messages up to %d NACKed.  Messages lost.", deliveryTag));
                else {
                    logger.info(String.format("Message %d NACKed.  Message lost.", deliveryTag));
                }
                // TODO: what else needed here?  The queue rejected it because the queue it full.
                // we can hang onto it in Metacat in some sort of backlog in-memory queue,
                // but that could get swamped too.
            }            
        });
        
    }
    
    /**
     * Submit a persistent message to the given queue in the broker, with publisher confirmations.
     * each submission will be assigned a deliveryTag (long), which will be logged on the same line
     * as a serialized form of the messageHeader map.
     * (A separate publish confirmation callback will log the deliveryTags with an ACK or NACK, on a 
     * separate log line.)
     * 
     * @param queueName
     * @param messageHeaders
     * @param body
     * @return
     * @throws IOException
     */
    public boolean submit(String queueName, Map<String, Object> messageHeaders, byte[] body) throws IOException {
        boolean success = false;
        
        long sequenceTag = 0;
        try {
            
            // declare the queue in case it is not there. 
            // TODO:  not sure if this what we want to do.  Maybe we should fail fast?
            // otherwise we are quietly submitting messages to a queue that will never be read.  
            confirmModeChannel.queueDeclare(queueName, QUEUEDURABLE, QUEUEEXCLUSIVE, QUEUEAUTODELETE, QUEUEARGUMENTS);
            
            // assemble the properties from the headers and things we know we need (persistent flag)
            BasicProperties props = new AMQP.BasicProperties.Builder()
            .deliveryMode(2)  // persistent message
            .headers(messageHeaders)
            .build();
            
      
            confirmModeChannel.basicPublish("", queueName, props, body);
            logger.info("MessageSubmitter.submit - successfully submit a message to the broker");
            success = true;
        } 
        finally {
            
            // assemble the log line that maps the headers to the delivery tag (assume it will have the pid).
            sequenceTag = confirmModeChannel.getNextPublishSeqNo();
            StringBuffer headers = new StringBuffer();
            if (messageHeaders != null) {
                for(Entry<String,Object> e : messageHeaders.entrySet()) {
                    headers.append("[ ");
                    headers.append(e.getKey());
                    headers.append(" = ");
                    headers.append(e.getValue().toString());
                    headers.append(" ]");
                }
            }
            
            logger.info(String.format("Publishing message with deliveryTag: %d , Headers %s, exit status: %",
                    sequenceTag, 
                    headers.toString(),
                    success ? "basicPublish succeeded" : "error at / before publish"));
 
        }
        return success;
    }
    
    /**
     * A method to close the connection. This method needs to be called in 
     * @throws IOException
     */
    public void closeConnection() throws IOException {
 
        if(confirmModeChannel != null) {
            try {
                confirmModeChannel.close();
            } catch (Exception e) {
                logger.warn("MessageSubmitter.closeConnection - could not close the channel. got exception: [ " 
                        + e.getClass().getSimpleName() + "] " +   e.getMessage(), e);
            }
            
        }
        if(connection != null) {
            logger.info("MessageSubmitter.closeConnection - close the connection ...");
            connection.close();
            logger.info("MessageSubmitter.closeConnection - close the connection sucessfully");
        } else {
            logger.info("MessageSubmitter.closeConnection - the connection wasn't initialized and we don't need to close it at all.");
        }
    }
    
    public Connection getConnection() {
        return connection;
    }

    public void setConnection(Connection connection) {
        this.connection = connection;
    }
}
