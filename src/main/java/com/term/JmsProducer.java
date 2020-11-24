package com.term;

import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

public class JmsProducer {
    private static final String MQ_URL = "tcp://192.168.31.150:61616";
    public static void main(String[] args) {
        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(MQ_URL);
        Connection connection = null;
        try {
            connection = connectionFactory.createConnection();
            connection.start();
            /*
                第一个参数事务
                第二个参数签收
             */
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            //5、使用会话对象创建目标对象，包含queue和topic（一对一和一对多）
            Queue queue = session.createQueue("test-queue");
            //6、使用会话对象创建生产者对象
            MessageProducer producer = session.createProducer(queue);
            //7、使用会话对象创建一个消息对象
            TextMessage textMessage = session.createTextMessage("hello!test-queue2");
            //8、发送消息
            producer.send(textMessage);
            //9、关闭资源
            producer.close();
            session.close();
            connection.close();
        } catch (JMSException e) {
            System.err.println(e);
        }
    }
}
