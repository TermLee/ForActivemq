package com.term.topic;

import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import java.io.IOException;
import java.util.Properties;

public class TopicConsumer2 {
    public static void main(String[] args) throws IOException {
        final Properties properties = new Properties();
        properties.load(TopicProducer.class.getResourceAsStream("/config.properties"));
        //1、创建工厂连接对象，需要制定ip和端口号
        final ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(properties.getProperty("MQ_URL"));
        //2、使用连接工厂创建一个连接对象
        Connection connection = null;
        try {
            connection = connectionFactory.createConnection();
            //3、开启连接
            connection.start();
            //4、使用连接对象创建会话（session）对象
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            //5、使用会话对象创建目标对象，包含queue和topic（一对一和一对多）
            Topic topic = session.createTopic(properties.getProperty("TOPIC_NAME"));
            MessageConsumer consumer = session.createConsumer(topic);
            consumer.setMessageListener(message -> {
                if (message != null && message instanceof TextMessage) {
                    try {
                        System.out.println("*" + ((TextMessage) message).getText());
                    } catch (JMSException e) {
                        System.out.println(e);
                    }
                }
            });
            System.in.read();
            consumer.close();
            session.close();
            connection.close();
        } catch (JMSException e) {
            e.printStackTrace();
        }
    }
}
