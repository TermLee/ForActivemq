package com.term.persistence;

import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import java.io.IOException;
import java.util.Properties;

public class TopicProducerPersistence {
    public static void main(String[] args) {
        final Properties properties = new Properties();
        try {
            properties.load(TopicProducerPersistence.class.getResourceAsStream("/config.properties"));
        } catch (IOException e) {
            System.out.println(e);
        }
        final String url = properties.getProperty("MQ_URL");
        final String topicName = properties.getProperty("TOPIC_NAME");

        //1、创建工厂连接对象，需要制定ip和端口号
        ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(url);
        //2、使用连接工厂创建一个连接对象
        Connection connection = null;
        try {
            connection = connectionFactory.createConnection();
            connection.setClientID("z3");
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Topic topic = session.createTopic(topicName);
            MessageProducer producer = session.createProducer(topic);
            producer.setDeliveryMode(DeliveryMode.PERSISTENT);
            connection.start();

            TextMessage textMessage = session.createTextMessage("just test!!");
            producer.send(textMessage);
            System.out.println("tui送成功！");

            producer.close();
            session.close();
            connection.close();
        } catch (JMSException e) {
            e.printStackTrace();
        }
    }
}
