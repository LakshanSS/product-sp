package org.wso2.sp.scenario;

import org.testng.Assert;
import org.testng.annotations.Test;
import org.wso2.sp.scenario.test.common.utils.http.source.HttpTestUtil;
import org.wso2.sp.scenario.utils.JMSClient;
import org.wso2.sp.scenario.utils.ResultContainer;
import java.net.URI;
import java.util.Random;

public class ReceiveHTTPStreamProcessItAndSendResultViaJMSTest {
    public static final String WORKER_IP = "localhost";
    public static final int WORKER_PORT = 9443;

    public static final String HTTP_SOURCE_IP = "localhost";
    public static final int HTTP_SOURCE_PORT = 8280;

    @Test(description = "2.3.2.1")
    public void sendResultToJMSTopic() throws InterruptedException{
        ResultContainer resultContainer = new ResultContainer(20);
        JMSClient client = new JMSClient("activemq", "JMS_SINK_TOPIC_TEST", "", resultContainer);
        try {
            Thread listenerThread = new Thread(client);
            listenerThread.start();
            Thread.sleep(1000);
            // Deploy SiddhiApp
            URI workerBaseURI = URI.create(String.format("https://%s:%d", WORKER_IP, WORKER_PORT));
            String path = "/siddhi-apps";
            String contentType = "text/plain";
            String method = "POST";
            String inStreamDefinition = "@App:name('TestSiddhiApp1')" +
                    "@source(type='http', @map(type='xml') )" +
                    "define stream inputStream (name string, amount double, timestamp long);";

            String outStreamDefinition = "@sink(type='jms', @map(type='xml'), "
                    + "factory.initial='org.apache.activemq.jndi.ActiveMQInitialContextFactory', "
                    + "provider.url='tcp://localhost:61616',"
                    + "destination='JMS_SINK_TOPIC_TEST',"
                    + "connection.factory.jndi.name='TopicConnectionFactory',"
                    + "connection.factory.type='topic',"
                    + "subscription.durable='true' "
                    + ")" +
                    "define stream outputStream (name string, amount double);";

            String query = ("@info(name = 'query') "
                    + "from inputStream#log('AA') "
                    + "select name, amount "
                    + "insert into outputStream;"
            );

            HttpTestUtil util = new HttpTestUtil();
            int response = util.httpsRequest(inStreamDefinition + outStreamDefinition + query, workerBaseURI, path,
                    true, false, method, contentType, "admin", "admin");
            Assert.assertEquals(response, 201, "Failed to deploy Siddhi App");
            Thread.sleep(5000);

            //Send events to the HTTP Source
            URI sourceURI = URI.create(String.format("http://%s:%d", HTTP_SOURCE_IP, HTTP_SOURCE_PORT));
            Random rand = new Random();
            for (int i = 0; i < 20; i++) {
                String name = "John" + rand.nextInt(10);
                int amount = rand.nextInt(99);
                String event = "<events>"
                        + "<event>"
                        + "<name>" + name + "</name>"
                        + "<amount>" + amount + "</amount>"
                        + "<timestamp>" + System.currentTimeMillis() + "</timestamp>"
                        + "</event>"
                        + "</events>";
                util.httpRequest(event, sourceURI, "/TestSiddhiApp1/inputStream", false, true,
                        "POST", "text/xml", null, null);
                Thread.sleep(20);
            }
            Thread.sleep(100);
            int responseDel = util.httpsRequest(null, workerBaseURI, path + "/TestSiddhiApp1", true,
                    false, "DELETE", "text/plain", "admin", "admin");
            Assert.assertEquals(responseDel, 200, "Failed to delete Siddhi App");
            Assert.assertTrue(resultContainer.assertMessageContent("John"));
        } finally {
            client.shutdown();
        }
    }

    @Test(description = "2.3.2.2")
    public void sendResultToJMSQueue() throws InterruptedException {
        ResultContainer resultContainer = new ResultContainer(20);
        JMSClient client = new JMSClient("activemq", "", "JMS_SINK_QUEUE_TEST", resultContainer);
        try {
            Thread listenerThread = new Thread(client);
            listenerThread.start();
            Thread.sleep(1000);
            // Deploy SiddhiApp
            URI workerBaseURI = URI.create(String.format("https://%s:%d", WORKER_IP, WORKER_PORT));
            String path = "/siddhi-apps";
            String contentType = "text/plain";
            String method = "POST";
            String inStreamDefinition = "@App:name('TestSiddhiApp1')" +
                    "@source(type='http', @map(type='xml') )" +
                    "define stream inputStream (name string, amount double, timestamp long);";

            String outStreamDefinition = "@sink(type='jms', @map(type='json'), "
                    + "factory.initial='org.apache.activemq.jndi.ActiveMQInitialContextFactory', "
                    + "provider.url='tcp://localhost:61616',"
                    + "destination='JMS_SINK_QUEUE_TEST' "
                    + ")" +
                    "define stream outputStream (name string, amount double);";

            String query = ("@info(name = 'query') "
                    + "from inputStream#log('AA') "
                    + "select name, amount "
                    + "insert into outputStream;"
            );

            HttpTestUtil util = new HttpTestUtil();
            int response = util.httpsRequest(inStreamDefinition + outStreamDefinition + query, workerBaseURI, path,
                    true, false, method, contentType, "admin", "admin");
            Assert.assertEquals(response, 201, "Failed to deploy Siddhi App");
            Thread.sleep(5000);

            // Send events to the HTTP Source
            URI sourceURI = URI.create(String.format("http://%s:%d", HTTP_SOURCE_IP, HTTP_SOURCE_PORT));
            Random rand = new Random();
            for (int i = 0; i < 20; i++) {
                String name = "John" + rand.nextInt(10);
                int amount = rand.nextInt(99);
                String event = "<events>"
                        + "<event>"
                        + "<name>" + name + "</name>"
                        + "<amount>" + amount + "</amount>"
                        + "<timestamp>" + System.currentTimeMillis() + "</timestamp>"
                        + "</event>"
                        + "</events>";
                util.httpRequest(event, sourceURI, "/TestSiddhiApp1/inputStream", false, true,
                        "POST", "text/xml", null, null);
                Thread.sleep(20);
            }
            Thread.sleep(100);
            int responseDel = util.httpsRequest(null, workerBaseURI, path + "/TestSiddhiApp1", true,
                    false, "DELETE", "text/plain", "admin", "admin");
            Assert.assertEquals(responseDel, 200, "Failed to delete Siddhi App");
            Assert.assertTrue(resultContainer.assertMessageContent("John"));
        } finally {
            client.shutdown();
        }
    }

    @Test(description = "2.3.2.3")
    public void sendResultToSecuredJMSBroker() {
        // TODO: 28/02/19  
    }

}
