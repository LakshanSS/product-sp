package org.wso2.sp.scenario.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import javax.jms.BytesMessage;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.QueueConnection;
import javax.jms.QueueConnectionFactory;
import javax.jms.Session;
import javax.jms.TextMessage;

public class QueueConsumer implements Runnable{
    private final ResultContainer resultContainer;
    private QueueConnectionFactory queueConnectionFactory;
    private String queueName;
    private boolean active = true;
    private static Logger log = LoggerFactory.getLogger(QueueConsumer.class);

    public QueueConsumer(QueueConnectionFactory queueConnectionFactory, String queueName,
                         ResultContainer resultContainer) {
        this.queueConnectionFactory = queueConnectionFactory;
        this.queueName = queueName;
        this.resultContainer = resultContainer;
    }

    public void run() {
        // create queue connection
        QueueConnection queueConnection = null;
        try {
            queueConnection = queueConnectionFactory.createQueueConnection();
            queueConnection.start();
        } catch (JMSException e) {
            log.error("Can not create queue connection." + e.getMessage(), e);
            return;
        }
        Session session;
        try {
            session = queueConnection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
            Destination destination = session.createQueue(queueName);
            MessageConsumer consumer = session.createConsumer(destination);
            log.info("Listening for messages at " + queueName);
            while (active) {
                Message message = consumer.receive(1000);
                if (message != null) {
                    if (message instanceof MapMessage) {
                        MapMessage mapMessage = (MapMessage) message;
                        Map<String, Object> map = new HashMap<String, Object>();
                        Enumeration enumeration = mapMessage.getMapNames();
                        while (enumeration.hasMoreElements()) {
                            String key = (String) enumeration.nextElement();
                            map.put(key, mapMessage.getObject(key));
                        }
                        resultContainer.eventReceived(map.toString());
                        log.info("Received Map Message: " + map);
                    } else if (message instanceof TextMessage) {
                        resultContainer.eventReceived(message.toString());
                        log.info("Received Text Message: " + ((TextMessage) message).getText());
                    } else if (message instanceof BytesMessage) {
                        byte[] byteData = null;
                        byteData = new byte[(int) ((BytesMessage) message).getBodyLength()];
                        ((BytesMessage) message).readBytes(byteData);
                        ((BytesMessage) message).reset();
                        resultContainer.eventReceived(new String(byteData));
                    } else {
                        resultContainer.eventReceived(message.toString());
                        log.info("Received message: " + message.toString());
                    }
                }
            }
            log.info("Finished listening for messages.");
            session.close();
            queueConnection.stop();
            queueConnection.close();
        } catch (JMSException e) {
            log.error("Can not subscribe." + e.getMessage(), e);
        }
    }

    public void shutdown() {
        active = false;
    }

}
