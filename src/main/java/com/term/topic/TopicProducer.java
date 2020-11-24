package com.term.topic;

import com.term.JmsConsumer;
import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import java.io.IOException;
import java.util.Properties;

public class TopicProducer {
    public static void main(String[] args) throws IOException {
        Properties properties = new Properties();
        properties.load(TopicProducer.class.getResourceAsStream("/config.properties"));
        //1、创建工厂连接对象，需要制定ip和端口号
        ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(properties.getProperty("MQ_URL"));
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
            //6、使用会话对象创建生产者对象
            MessageProducer producer = session.createProducer(topic);
            int i = 0;
            while (true){
                //7、使用会话对象创建一个消息对象
                TextMessage textMessage = session.createTextMessage("hello!test-topic" + i++);
                //8、发送消息
                producer.send(textMessage);
                System.out.println("已发送"+i);
                if (i>100)
                    break;
                Thread.sleep(1000l);
            }
            //9、关闭资源
            producer.close();
            session.close();
            connection.close();
        } catch (JMSException | InterruptedException e) {
            e.printStackTrace();
        }

    }
}
