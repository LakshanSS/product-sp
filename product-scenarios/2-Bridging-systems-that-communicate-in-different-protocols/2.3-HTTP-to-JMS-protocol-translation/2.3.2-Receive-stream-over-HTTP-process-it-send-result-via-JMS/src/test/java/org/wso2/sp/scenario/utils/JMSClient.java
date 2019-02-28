package org.wso2.sp.scenario.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;
import javax.jms.QueueConnectionFactory;
import javax.jms.TopicConnectionFactory;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

public class JMSClient implements Runnable{
    private static Logger log = LoggerFactory.getLogger(JMSClient.class);
    private final ResultContainer resultContainer;

    private String broker, topic, queue;
    private TopicConsumer topicConsumer;
    private QueueConsumer queueConsumer;


    public JMSClient(String broker, String topic, String queue, ResultContainer resultContainer) {
        this.broker = broker;
        this.topic = topic;
        this.queue = queue;
        this.resultContainer = resultContainer;
    }

    public void listen()  {
        Properties properties = new Properties();
        try {
            if ("qpid".equalsIgnoreCase(broker)) {
                properties.load(ClassLoader.getSystemClassLoader().getResourceAsStream("qpid.properties"));
            } else if ("activemq".equalsIgnoreCase(broker)) {
                properties.load(ClassLoader.getSystemClassLoader().getResourceAsStream("activemq.properties"));
            } else if ("mb".equalsIgnoreCase(broker)) {
                properties.load(ClassLoader.getSystemClassLoader().getResourceAsStream("mb.properties"));
            } else {
                log.error("Entered broker is invalid! ");
            }
            if (topic != null && topic.isEmpty() || topic.equals("\"\"")) {
                topic = null;
            }
            if (queue != null && queue.isEmpty() || queue.equals("\"\"")) {
                queue = null;
            }
            if (topic == null && queue == null) {
                log.error("Enter topic value or queue value! ");
            } else if (topic != null) {
                Context context = new InitialContext(properties);
                TopicConnectionFactory topicConnectionFactory =
                        (TopicConnectionFactory) context.lookup("ConnectionFactory");
                topicConsumer = new TopicConsumer(topicConnectionFactory, topic, resultContainer);
                Thread consumerThread = new Thread(topicConsumer);
                log.info("Starting" + broker + "consumerTopic thread...");
                consumerThread.start();
            } else {
                Context context = new InitialContext(properties);
                QueueConnectionFactory queueConnectionFactory =
                        (QueueConnectionFactory) context.lookup("ConnectionFactory");
                queueConsumer = new QueueConsumer(queueConnectionFactory, queue, resultContainer);
                Thread consumerThread = new Thread(queueConsumer);
                log.info("Starting" + broker + "consumerQueue thread...");
                consumerThread.start();
            }
        } catch (IOException e) {
            log.error("Cannot read properties file from resources. " + e.getMessage(), e);
        } catch (NamingException e) {
            log.error("Invalid properties in the properties " + e.getMessage(), e);
        }
    }

    @Override
    public void run() {
        try {
            listen();
        } catch (Exception e) {
            log.error("Error starting the JMS consumer: ", e);
        }
    }

    public void shutdown() {
        if (topicConsumer != null) {
            topicConsumer.shutdown();
        } else {
            queueConsumer.shutdown();
        }
    }
}
