package org.dataone.cn.index.messaging.rabbitmq;

import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;



//@Configuration
public class MessagingServerConfiguration {

    @Bean
    public CachingConnectionFactory rabbitConnectionFactory() {
        CachingConnectionFactory connectionFactory =
                new CachingConnectionFactory("localhost");
        connectionFactory.setUsername("guest");
        connectionFactory.setPassword("guest");

        connectionFactory.setPublisherConfirms(true);
        connectionFactory.setPublisherReturns(false);
        
        return connectionFactory;
    }
    
    @Bean
    public RabbitAdmin rabbitAdmin() {
        return new RabbitAdmin(rabbitConnectionFactory());
    }

    @Bean
    public Queue newTaskQueue() {
        Queue queue = new Queue("indexing.newTaskQueue");
        rabbitAdmin().declareQueue(queue);
        return queue;
    }

}
