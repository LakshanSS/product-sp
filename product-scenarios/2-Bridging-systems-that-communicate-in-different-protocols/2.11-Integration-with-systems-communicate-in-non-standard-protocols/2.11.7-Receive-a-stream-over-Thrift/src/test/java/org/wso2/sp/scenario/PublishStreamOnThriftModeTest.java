package org.wso2.sp.scenario;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;
import org.wso2.sp.scenario.test.common.utils.http.sink.HttpServerListenerHandler;
import org.wso2.sp.scenario.test.common.utils.http.source.HttpTestUtil;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

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
        URI workerBaseURI = URI.create(String.format("https://%s:%d", WORKER_IP, WORKER_PORT));
        String path = "/siddhi-apps";
        String contentType = "text/plain";
        String method = "POST";
        String inStreamDefinition = "@App:name('TestSiddhiApp1')" +
                "@source(type='http', @map(type='xml') )" +
                "define stream inputStream (name string, amount double, timestamp long);";

        String inStreamDefinition2 = "@source(type='wso2event',wso2.stream.id='outputStream:1.0.0', "
                +"@map(type='wso2event') )" +
                "define stream FooStream (name string, amount double);";

        String outStreamDefinition = String.format("@sink(type='http',publisher.url='http://%s:%d/abc',method='POST'," +
                "headers='Content-Type:xml',@map(type='xml', @payload('<name>{{name}}</name><amount>{{amount}}rs" +
                "</amount>'))) define stream outputStream (name string, amount double);", HTTP_SINK_IP, HTTP_SINK_PORT);

        String query = ("@info(name = 'query') "
                + "from inputStream#log('AA') "
                + "select name, amount "
                + "insert into FooStream;"
        );

        String query2 = ("@info(name = 'query2') "
                + "from FooStream#log('BB') "
                + "select name, amount "
                + "insert into outputStream;"
        );

        HttpTestUtil util = new HttpTestUtil();
        int response = util.httpsRequest(inStreamDefinition + inStreamDefinition2 + outStreamDefinition + query
                        + query2, workerBaseURI, path, true, false, method, contentType, "admin", "admin");
        Assert.assertEquals(response, 201, "Failed to deploy Siddhi App");
        Thread.sleep(5000);
        URI sourceURI = URI.create(String.format("http://%s:%d", HTTP_SOURCE_IP, HTTP_SOURCE_PORT));

        List<String> expected = new ArrayList<>(2);

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
                    + "<timestamp>" + System.currentTimeMillis() + "</timestamp>"
                    + "</event>"
                    + "</events>";
            util.httpRequest(event, sourceURI, "/TestSiddhiApp1/inputStream", false, true,
                    "POST", "text/xml", null, null);
            String expectedEvent = "<name>" + name + "</name>"
                    + "<amount>" + amount + "rs</amount>";
            expected.add(expectedEvent);
            Thread.sleep(20);
        }
        Thread.sleep(100);
        int responseDel = util.httpsRequest(null, workerBaseURI, path + "/TestSiddhiApp1", true,
                false, "DELETE", "text/plain", "admin", "admin");
        Assert.assertEquals(responseDel, 200, "Failed to delete Siddhi App");
        Assert.assertEquals(lst.getServerListener().getData(), expected, "Expected data not matched!");
        lst.shutdown();
    }
}
