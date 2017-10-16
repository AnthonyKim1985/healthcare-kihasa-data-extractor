package org.bigdatacenter.healthcarekihasadataextractor.config;

import org.bigdatacenter.healthcarekihasadataextractor.rabbitmq.RabbitMQReceiverImpl;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.amqp.rabbit.annotation.EnableRabbit;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.adapter.MessageListenerAdapter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableRabbit
public class RabbitMQConfig {
    public final static String EXTRACTION_REQUEST_QUEUE = "kihasa-extractor-queue";

    private final ConnectionFactory connectionFactory;

    @Autowired
    public RabbitMQConfig(ConnectionFactory connectionFactory) {
        this.connectionFactory = connectionFactory;
    }

    @Bean
    public Queue queue() {
        return new Queue(EXTRACTION_REQUEST_QUEUE, false);
    }

    @Bean
    public TopicExchange exchange() {
        return new TopicExchange(EXTRACTION_REQUEST_QUEUE + "-exchange");
    }

    @Bean
    public Binding binding(Queue queue, TopicExchange exchange) {
        return BindingBuilder.bind(queue).to(exchange).with(EXTRACTION_REQUEST_QUEUE);
    }

    @Bean
    public RabbitAdmin rabbitAdmin() {
        return new RabbitAdmin(connectionFactory);
    }

    @Bean
    public SimpleMessageListenerContainer container(ConnectionFactory connectionFactory, MessageListenerAdapter listenerAdapter) {
        SimpleMessageListenerContainer container = new SimpleMessageListenerContainer();
        container.setConnectionFactory(connectionFactory);
        container.setQueueNames(EXTRACTION_REQUEST_QUEUE);
        container.setMessageListener(listenerAdapter);
        container.setConcurrentConsumers(1);
        container.setMaxConcurrentConsumers(2);
        container.setReceiveTimeout(3000L);
        container.setRecoveryInterval(3000L);

        return container;
    }

    @Bean
    public MessageListenerAdapter listenerAdapter(RabbitMQReceiverImpl rabbitMQReceiverImpl) {
        return new MessageListenerAdapter(rabbitMQReceiverImpl, "runReceiver");
    }
}
