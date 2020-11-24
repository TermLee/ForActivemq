package com.term;

import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.io.IOException;
import java.util.Properties;

public class JmsConsumer {
    private static final String MQ_URL = "tcp://192.168.31.150:61616";
    public static void main(String[] args) throws ClassNotFoundException, IOException {
        Properties properties = new Properties();
        properties.load(JmsConsumer.class.getResourceAsStream("/config.properties"));
        ActiveMQConnectionFactory connectionFactory =
                new ActiveMQConnectionFactory(properties.getProperty("MQ_URL"));
        Connection connection = null;
        try {
            connection = connectionFactory.createConnection();
            connection.start();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue(properties.getProperty("QUEUE1"));
            MessageConsumer consumer = session.createConsumer(queue);
//            while (true) {
            /**
             * 这是第一种接受方式，receive()方法会一直等待接受消息。阻塞线程
             * receive(times)会等待times的时间，过期不候
             */
//                TextMessage textMessage = (TextMessage) consumer.receive();
//                if (textMessage == null){
//                    break;
//                }
//                System.out.println(textMessage.getText());
//            }

            consumer.setMessageListener(message -> {
                if (message != null && message instanceof TextMessage)
                {
                    try
                    {
                        System.out.println(((TextMessage) message).getText());
                    }
                    catch (JMSException e)
                    {
                        System.out.println(e);
                    }
                }
            });
            System.in.read();

            session.close();
            consumer.close();
            connection.close();

        } catch (JMSException e) {
            e.printStackTrace();
        }
    }
}
