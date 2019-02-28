package org.wso2.sp.scenario;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;
import org.wso2.extension.siddhi.io.tcp.transport.TCPNettyClient;
import org.wso2.extension.siddhi.map.binary.sinkmapper.BinaryEventConverter;
import org.wso2.extension.siddhi.map.binary.utils.EventDefinitionConverterUtil;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.SiddhiApp;
import org.wso2.siddhi.query.compiler.SiddhiCompiler;
import org.wso2.sp.scenario.test.common.utils.http.sink.HttpServerListenerHandler;
import org.wso2.sp.scenario.test.common.utils.http.source.HttpTestUtil;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;

public class ReceiveStreamOverTCP {
    public static final String WORKER_IP = "localhost";
    public static final int WORKER_PORT = 9443;

    public static final String HTTP_SINK_IP = "localhost";
    public static final int HTTP_SINK_PORT = 8005;

    private static Logger log = LoggerFactory.getLogger(ReceiveStreamOverTCP.class);

    @Test(description = "2.4.2.1")
    public void receiveStreamOverTCP() throws Exception {
        URI workerBaseURI = URI.create(String.format("https://%s:%d", WORKER_IP, WORKER_PORT));
        String path = "/siddhi-apps";
        String contentType = "text/plain";
        String method = "POST";

        //Siddhi App Creation
        String inStreamDefinition = "@App:name('TestSiddhiApp1')" +
                "@source(type='tcp', @map(type='binary'))" +
                "define stream inputStream (name string, amount double);";

        String outStreamDefinition = String.format("@sink(type='http',publisher.url='http://%s:%d/abc',method='POST'," +
                "headers='Content-Type:xml',@map(type='xml', @payload('<name>{{name}}</name><amount>{{amount}}rs" +
                "</amount>'))) define stream outputStream (name string, amount double);", HTTP_SINK_IP, HTTP_SINK_PORT);

        String query = ("@info(name = 'query') "
                + "from inputStream#log('AA') "
                + "select * "
                + "insert into outputStream;"
        );

        SiddhiApp siddhiApp = SiddhiCompiler.parse(inStreamDefinition + outStreamDefinition + query);
        Attribute.Type[] types = EventDefinitionConverterUtil.generateAttributeTypeArray(siddhiApp
                .getStreamDefinitionMap().get("inputStream").getAttributeList());

        //Deploy Siddhi App
        HttpTestUtil util = new HttpTestUtil();
        int response = util.httpsRequest(inStreamDefinition + outStreamDefinition + query, workerBaseURI, path,
                true, false, method, contentType, "admin", "admin");
        Assert.assertEquals(response, 201, "Failed to deploy Siddhi App");
        Thread.sleep(5000);

        HttpServerListenerHandler lst = new HttpServerListenerHandler(HTTP_SINK_PORT);
        lst.run();

        TCPNettyClient tcpNettyClient = new TCPNettyClient();
        tcpNettyClient.connect("localhost", 9892);
        ArrayList<Event> arrayList = new ArrayList<Event>(3);

        arrayList.add(new Event(System.currentTimeMillis(), new Object[]{"John1", 10.8}));
        arrayList.add(new Event(System.currentTimeMillis(), new Object[]{"John2", 20.6}));
        arrayList.add(new Event(System.currentTimeMillis(), new Object[]{"John3", 30.5}));

        try {
            tcpNettyClient.send("TestSiddhiApp1/inputStream", BinaryEventConverter.convertToBinaryMessage(
                    arrayList.toArray(new Event[3]), types).array()).await();
        } catch (IOException e) {
            log.error(e.getMessage());
        }

        tcpNettyClient.disconnect();
        tcpNettyClient.shutdown();
        Thread.sleep(300);
        int responseDel = util.httpsRequest(null, workerBaseURI, path + "/TestSiddhiApp1", true,
                false, "DELETE", "text/plain", "admin", "admin");
        Assert.assertEquals(responseDel, 200, "Failed to delete Siddhi App");
        log.info(lst.getServerListener().getData().toString());
        Assert.assertTrue(lst.getServerListener().getData().toString().contains("John1"));
        lst.shutdown();
    }
}
