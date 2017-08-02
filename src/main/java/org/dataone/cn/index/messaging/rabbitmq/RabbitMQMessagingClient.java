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

import org.apache.log4j.Logger;
import org.dataone.cn.index.messaging.IndexTaskMessagingClient;
import org.dataone.cn.index.task.IndexTask;
import org.dataone.cn.messaging.QueueAccess;
import org.dataone.configuration.Settings;
import org.dataone.service.exceptions.InvalidSystemMetadata;
import org.dataone.service.exceptions.ServiceFailure;
import org.dataone.service.types.v2.SystemMetadata;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageBuilder;
import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;





/**
 * Represents a client to submit index tasks to a RabbitMQ message server
 * @author tao
 *
 */
public class RabbitMQMessagingClient implements IndexTaskMessagingClient {
    
    
    
    private static CachingConnectionFactory rabbitConnectionFactory = null;
   
    private Queue newTaskQueue = null;
    
    private QueueAccess queueAccess = null;
    
    private static Logger logger = Logger.getLogger(RabbitMQMessagingClient.class.getName());
    
    /**
     * Default constructor
     */
    public RabbitMQMessagingClient() {
        initConnFactory();
        initQueue();
        queueAccess = new QueueAccess(rabbitConnectionFactory, newTaskQueue.getName());
    }
    
    /**
     * Initialize the caching connection factory base on the configuration
     */
    private void initConnFactory() {
        String username = Settings.getConfiguration().getString("messaging.username");
        logger.info("RabbitMQMessagingClient.initConnFactory - the user name of the connection is "+username);
        String password = Settings.getConfiguration().getString("messaging.password");
        String hostname = Settings.getConfiguration().getString("messaging.hostname");
        logger.info("RabbitMQMessagingClient.initConnFactory - the host name of the connection is "+hostname);
        rabbitConnectionFactory = new CachingConnectionFactory(hostname);
        rabbitConnectionFactory.setUsername(username);
        rabbitConnectionFactory.setPassword(password);
        rabbitConnectionFactory.setPublisherConfirms(true);
        rabbitConnectionFactory.setPublisherReturns(false);
    }
    
    /**
     * Initialize the new task queue base on the configuration
     * This method should be called after calling the method initConnFactory
     */
    private void initQueue() {
        String newTaskQueueName = Settings.getConfiguration().getString("messaging.newtask.queuename");
        logger.info("RabbitMQMessagingClient.initQueue - the name of the new task queue is "+newTaskQueueName);
        RabbitAdmin rabbitAdmin =  new RabbitAdmin(rabbitConnectionFactory);
        //The queue is durable, non-exclusive and non auto-delete.
        newTaskQueue = new Queue(newTaskQueueName);
        rabbitAdmin.declareQueue(newTaskQueue);
    }
    
    /**
     * Submit a index task to the message server
     * @param indexTask
     * @return
     * @throws ServiceFailure
     */
    @Override
    public boolean submit(IndexTask indexTask) throws ServiceFailure, InvalidSystemMetadata {
        boolean success = false;
        if(indexTask == null) {
            throw new IllegalArgumentException("RabbitMQMessagingClient.submit - the paramater of the IndexTask object can't be null.");
        }
        queueAccess.publish(generateMessage(indexTask));
        return success;
    }
    
    /**
     * Generate a Message object from the given IndexTask object
     * @param indexTask
     * @return the rabbitMQ message object
     */
    private Message generateMessage(IndexTask indexTask) throws ServiceFailure, InvalidSystemMetadata {
        if(indexTask == null) {
            throw new IllegalArgumentException("RabbitMQMessagingClient.generateMesage - the paramater of the IndexTask object can't be null.");
        }
        SystemMetadata sysmetaObj = indexTask.unMarshalSystemMetadata();
        if(sysmetaObj == null) {
            throw new InvalidSystemMetadata("0000", "The system metadata string in the index task can't be transformed to a java object. The system metadata string is: \n"+indexTask.getSysMetadata());
        }
        byte[] body = null;
        try {
            body = getBytes(indexTask);
        } catch (IOException e) {
            logger.error("RabbitMQMessagingClient.submit - couldn't transform the index task object for the id "+indexTask.getPid()+" to a byte array since "+e.getMessage(), e);
            throw new ServiceFailure("0000", e.getMessage());
        }
        String originalNodeId = "unknown";
        if(sysmetaObj.getOriginMemberNode() != null) {
            originalNodeId = sysmetaObj.getOriginMemberNode().getValue();
        }
        Message message = MessageBuilder.withBody(body)
                .setDeliveryMode(MessageDeliveryMode.PERSISTENT)
                .setHeader(NODEID, originalNodeId)
                .setHeader(FORMATTYPE, indexTask.getFormatId())
                .setHeader(PID, indexTask.getPid())
                .build();
        return message;
    }
    
    /**
     * Transform an IndexTask object to the byte array.
     * @param indexTask
     * @return
     * @throws IOException
     */
    private byte[] getBytes(IndexTask indexTask) throws IOException{
        byte[]bytes = null;
        if(indexTask == null) {
            throw new IllegalArgumentException("RabbitMQMessagingClient.getBytes - the paramater of the IndexTask object can't be null.");
        }
        bytes = indexTask.serialize();
        return bytes;
    }

}
