package org.wso2.sp.scenario;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;
import org.wso2.sp.scenario.test.common.utils.http.sink.HttpServerListenerHandler;
import org.wso2.sp.scenario.test.common.utils.http.source.HttpTestUtil;
import java.net.URI;
import java.util.*;

public class PublishStreamOnThriftModeTest {
    public static final String WORKER_IP = "localhost";
    public static final int WORKER_PORT = 9443;

    public static final String HTTP_SOURCE_IP = "localhost";
    public static final int HTTP_SOURCE_PORT = 8280;

    public static final String HTTP_SINK_IP = "localhost";
    public static final int HTTP_SINK_PORT = 8005;

    private static Logger log = LoggerFactory.getLogger(PublishStreamOnThriftModeTest.class);

    @Test(description = "2.11.18.1")
    public void publishStreamOnThriftModeTest() throws Exception {
        //SiddhiApp creation
        URI workerBaseURI = URI.create(String.format("https://%s:%d", WORKER_IP, WORKER_PORT));
        String path = "/siddhi-apps";
        String contentType = "text/plain";
        String method = "POST";
        String inStreamDefinition = "@App:name('TestSiddhiApp1')" +
                "@source(type='http', @map(type='xml') )" +
                "define stream inputStream (name string, amount double);";

        String inStreamDefinition2 = "@source(type='wso2event',wso2.stream.id='outputStream:1.0.0', "
                +"@map(type='wso2event') )" +
                "define stream FooStream (name string, amount double);";

        String outStreamDefinition = "@sink(type='wso2event', @map(type='wso2event') ,"
                + " username='admin',"
                + " password='admin',"
                + " url='tcp://localhost:7611',"
                + " wso2.stream.id='outputStream:1.0.0',"
                + " protocol='thrift')"
                + " define stream outputStream (name string, amount double);";

        String outStreamDefinition2 = String.format("@sink(type='http',publisher.url='http://%s:%d/abc',method='POST'," +
                "headers='Content-Type:xml',@map(type='xml', @payload('<name>{{name}}</name><amount>{{amount}}rs" +
                "</amount>'))) define stream BarStream (name string, amount double);", HTTP_SINK_IP, HTTP_SINK_PORT);

        String query = ("@info(name = 'query') "
                + "from inputStream#log('AA') "
                + "select name, amount "
                + "insert into outputStream;"
        );

        String query2 = ("@info(name = 'query2') "
                + "from FooStream#log('BB') "
                + "select name, amount "
                + "insert into BarStream;"
        );

        //Deploy Siddhi App
        HttpTestUtil util = new HttpTestUtil();
        int response = util.httpsRequest(inStreamDefinition + inStreamDefinition2 + outStreamDefinition
                        + outStreamDefinition2 + query + query2, workerBaseURI, path, true, false, method,
                contentType, "admin", "admin");
        Assert.assertEquals(response, 201, "Failed to deploy Siddhi App");
        Thread.sleep(5000);

        List<String> expected = new ArrayList<>(20);

        //Send events to HTTP source
        URI sourceURI = URI.create(String.format("http://%s:%d", HTTP_SOURCE_IP, HTTP_SOURCE_PORT));
        Random rand = new Random();
        HttpServerListenerHandler lst = new HttpServerListenerHandler(HTTP_SINK_PORT);
        lst.run();
        for (int i = 0; i < 20; i++) {
            String name = "John" + rand.nextInt(10);
            int amount = rand.nextInt(99);
            String event = "<events>"
                    + "<event>"
                    + "<name>" + name + "</name>"
                    + "<amount>" + amount + "</amount>"
                    + "</event>"
                    + "</events>";
            util.httpRequest(event, sourceURI, "/TestSiddhiApp1/inputStream", false, true,
                    "POST", "text/xml", null, null);
            Thread.sleep(20);
        }
        Thread.sleep(100);

        //Delete Siddhi App
        int responseDel = util.httpsRequest(null, workerBaseURI, path + "/TestSiddhiApp1", true,
                false, "DELETE", "text/plain", "admin", "admin");
        Assert.assertEquals(responseDel, 200, "Failed to delete Siddhi App");
        Assert.assertTrue(lst.getServerListener().getData().get(0).toString().contains("John"));
        lst.shutdown();
    }

    @Test(description = "2.11.18.2")
    public void publishEventsOnBinaryMode() throws Exception {
        //SiddhiApp creation
        URI workerBaseURI = URI.create(String.format("https://%s:%d", WORKER_IP, WORKER_PORT));
        String path = "/siddhi-apps";
        String contentType = "text/plain";
        String method = "POST";
        String inStreamDefinition = "@App:name('TestSiddhiApp1')" +
                "@source(type='http', @map(type='xml') )" +
                "define stream inputStream (name string, amount double);";

        String inStreamDefinition2 = "@source(type='wso2event',wso2.stream.id='outputStream:1.0.0', "
                +"@map(type='wso2event') )" +
                "define stream FooStream (name string, amount double);";

        String outStreamDefinition = "@sink(type='wso2event', @map(type='wso2event') ,"
                + " username='admin',"
                + " password='admin',"
                + " url='tcp://localhost:9612',"
                + " protocol='binary')"
                + " define stream outputStream (name string, amount double);";

        String outStreamDefinition2 = String.format("@sink(type='http',publisher.url='http://%s:%d/abc',method='POST'," +
                "headers='Content-Type:xml',@map(type='xml', @payload('<name>{{name}}</name><amount>{{amount}}rs" +
                "</amount>'))) define stream BarStream (name string, amount double);", HTTP_SINK_IP, HTTP_SINK_PORT);

        String query = ("@info(name = 'query') "
                + "from inputStream#log('AA') "
                + "select name, amount "
                + "insert into outputStream;"
        );

        String query2 = ("@info(name = 'query2') "
                + "from FooStream#log('BB') "
                + "select name, amount "
                + "insert into BarStream;"
        );

        //Deploy Siddhi App
        HttpTestUtil util = new HttpTestUtil();
        int response = util.httpsRequest(inStreamDefinition + inStreamDefinition2+ outStreamDefinition
                        + outStreamDefinition2 + query + query2, workerBaseURI, path, true, false, method,
                contentType, "admin", "admin");
        Assert.assertEquals(response, 201, "Failed to deploy Siddhi App");
        Thread.sleep(5000);

        //Send events to HTTP source
        URI sourceURI = URI.create(String.format("http://%s:%d", HTTP_SOURCE_IP, HTTP_SOURCE_PORT));
        Random rand = new Random();
        HttpServerListenerHandler lst = new HttpServerListenerHandler(HTTP_SINK_PORT);
        lst.run();
        for (int i = 0; i < 20; i++) {
            String name = "John" + rand.nextInt(10);
            int amount = rand.nextInt(99);
            String event = "<events>"
                    + "<event>"
                    + "<name>" + name + "</name>"
                    + "<amount>" + amount + "</amount>"
                    + "</event>"
                    + "</events>";
            util.httpRequest(event, sourceURI, "/TestSiddhiApp1/inputStream", false, true,
                    "POST", "text/xml", null, null);
            Thread.sleep(20);
        }
        Thread.sleep(100);

        //Delete Siddhi App
        int responseDel = util.httpsRequest(null, workerBaseURI, path + "/TestSiddhiApp1", true,
                false, "DELETE", "text/plain", "admin", "admin");
        Assert.assertEquals(responseDel, 200, "Failed to delete Siddhi App");
        Assert.assertTrue(lst.getServerListener().getData().get(0).toString().contains("John"));
        lst.shutdown();
    }

    @Test(description = "2.11.18.3")
    public void publishStreamInSyncManner() throws Exception {
        //SiddhiApp creation
        URI workerBaseURI = URI.create(String.format("https://%s:%d", WORKER_IP, WORKER_PORT));
        String path = "/siddhi-apps";
        String contentType = "text/plain";
        String method = "POST";
        String inStreamDefinition = "@App:name('TestSiddhiApp1')" +
                "@source(type='http', @map(type='xml') )" +
                "define stream inputStream (name string, amount double);";

        String inStreamDefinition2 = "@source(type='wso2event',wso2.stream.id='outputStream:1.0.0', "
                +"@map(type='wso2event') )" +
                "define stream FooStream (name string, amount double);";

        String outStreamDefinition = "@sink(type='wso2event', @map(type='wso2event') ,"
                + " username='admin',"
                + " password='admin',"
                + " url='tcp://localhost:7611',"
                + " mode='blocking')"
                + " define stream outputStream (name string, amount double);";

        String outStreamDefinition2 = String.format("@sink(type='http',publisher.url='http://%s:%d/abc',method='POST'," +
                "headers='Content-Type:xml',@map(type='xml', @payload('<name>{{name}}</name><amount>{{amount}}rs" +
                "</amount>'))) define stream BarStream (name string, amount double);", HTTP_SINK_IP, HTTP_SINK_PORT);

        String query = ("@info(name = 'query') "
                + "from inputStream#log('AA') "
                + "select name, amount "
                + "insert into outputStream;"
        );

        String query2 = ("@info(name = 'query2') "
                + "from FooStream#log('BB') "
                + "select name, amount "
                + "insert into BarStream;"
        );

        //Deploy Siddhi App
        HttpTestUtil util = new HttpTestUtil();
        int response = util.httpsRequest(inStreamDefinition + inStreamDefinition2 + outStreamDefinition
                        + outStreamDefinition2 + query + query2, workerBaseURI, path, true, false, method,
                contentType, "admin", "admin");
        Assert.assertEquals(response, 201, "Failed to deploy Siddhi App");
        Thread.sleep(5000);

        //Send events to HTTP source
        URI sourceURI = URI.create(String.format("http://%s:%d", HTTP_SOURCE_IP, HTTP_SOURCE_PORT));
        Random rand = new Random();
        HttpServerListenerHandler lst = new HttpServerListenerHandler(HTTP_SINK_PORT);
        lst.run();
        for (int i = 0; i < 20; i++) {
            String name = "John" + rand.nextInt(10);
            int amount = rand.nextInt(99);
            String event = "<events>"
                    + "<event>"
                    + "<name>" + name + "</name>"
                    + "<amount>" + amount + "</amount>"
                    + "</event>"
                    + "</events>";
            util.httpRequest(event, sourceURI, "/TestSiddhiApp1/inputStream", false, true,
                    "POST", "text/xml", null, null);
            Thread.sleep(20);
        }
        Thread.sleep(100);

        //Delete Siddhi App
        int responseDel = util.httpsRequest(null, workerBaseURI, path + "/TestSiddhiApp1", true,
                false, "DELETE", "text/plain", "admin", "admin");
        Assert.assertEquals(responseDel, 200, "Failed to delete Siddhi App");
        Assert.assertTrue(lst.getServerListener().getData().get(0).toString().contains("John"));
        lst.shutdown();
    }

    @Test(description = "2.11.18.4")
    public void publishStreamInAsyncManner() throws Exception {
        //SiddhiApp creation
        URI workerBaseURI = URI.create(String.format("https://%s:%d", WORKER_IP, WORKER_PORT));
        String path = "/siddhi-apps";
        String contentType = "text/plain";
        String method = "POST";
        String inStreamDefinition = "@App:name('TestSiddhiApp1')" +
                "@source(type='http', @map(type='xml') )" +
                "define stream inputStream (name string, amount double);";

        String inStreamDefinition2 = "@source(type='wso2event',wso2.stream.id='outputStream:1.0.0', "
                +"@map(type='wso2event') )" +
                "define stream FooStream (name string, amount double);";

        String outStreamDefinition = "@sink(type='wso2event', @map(type='wso2event') ,"
                + " username='admin',"
                + " password='admin',"
                + " url='tcp://localhost:7611',"
                + " mode='non-blocking')"
                + " define stream outputStream (name string, amount double);";

        String outStreamDefinition2 = String.format("@sink(type='http',publisher.url='http://%s:%d/abc',method='POST'," +
                "headers='Content-Type:xml',@map(type='xml', @payload('<name>{{name}}</name><amount>{{amount}}rs" +
                "</amount>'))) define stream BarStream (name string, amount double);", HTTP_SINK_IP, HTTP_SINK_PORT);

        String query = ("@info(name = 'query') "
                + "from inputStream#log('AA') "
                + "select name, amount "
                + "insert into outputStream;"
        );

        String query2 = ("@info(name = 'query2') "
                + "from FooStream#log('BB') "
                + "select name, amount "
                + "insert into BarStream;"
        );

        //Deploy Siddhi App
        HttpTestUtil util = new HttpTestUtil();
        int response = util.httpsRequest(inStreamDefinition + inStreamDefinition2 + outStreamDefinition
                        + outStreamDefinition2 + query + query2, workerBaseURI, path, true, false, method,
                contentType, "admin", "admin");
        Assert.assertEquals(response, 201, "Failed to deploy Siddhi App");
        Thread.sleep(5000);

        //Send events to HTTP source
        URI sourceURI = URI.create(String.format("http://%s:%d", HTTP_SOURCE_IP, HTTP_SOURCE_PORT));
        Random rand = new Random();
        HttpServerListenerHandler lst = new HttpServerListenerHandler(HTTP_SINK_PORT);
        lst.run();
        for (int i = 0; i < 20; i++) {
            String name = "John" + rand.nextInt(10);
            int amount = rand.nextInt(99);
            String event = "<events>"
                    + "<event>"
                    + "<name>" + name + "</name>"
                    + "<amount>" + amount + "</amount>"
                    + "</event>"
                    + "</events>";
            util.httpRequest(event, sourceURI, "/TestSiddhiApp1/inputStream", false, true,
                    "POST", "text/xml", null, null);
            Thread.sleep(20);
        }
        Thread.sleep(100);

        //Delete Siddhi App
        int responseDel = util.httpsRequest(null, workerBaseURI, path + "/TestSiddhiApp1", true,
                false, "DELETE", "text/plain", "admin", "admin");
        Assert.assertEquals(responseDel, 200, "Failed to delete Siddhi App");
        Assert.assertTrue(lst.getServerListener().getData().get(0).toString().contains("John"));
        lst.shutdown();
    }
}
