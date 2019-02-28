package org.wso2.sp.scenario;

import com.icegreen.greenmail.util.DummySSLSocketFactory;
import com.icegreen.greenmail.util.GreenMail;
import com.icegreen.greenmail.util.ServerSetupTest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;
import org.wso2.sp.scenario.test.common.utils.http.sink.HttpServerListenerHandler;
import org.wso2.sp.scenario.test.common.utils.http.source.HttpTestUtil;

import javax.mail.MessagingException;
import javax.mail.internet.MimeMessage;
import java.io.IOException;
import java.net.URI;
import java.security.Security;
import java.util.*;

import static org.testng.Assert.assertTrue;

public class ReceiveHTTPStreamAndGenerateEmailTest {
    public static final String WORKER_IP = "localhost";
    public static final int WORKER_PORT = 9443;

    public static final String HTTP_SOURCE_IP = "localhost";
    public static final int HTTP_SOURCE_PORT = 8280;

    public static final String HTTP_SINK_IP = "localhost";
    public static final int HTTP_SINK_PORT = 8005;

    private static final String PASSWORD = "password";
    private static final String USERNAME = "abc";
    private static final String ADDRESS = "abc@localhost";
    private GreenMail mailServer;

    private static Logger log = LoggerFactory.getLogger(ReceiveHTTPStreamAndGenerateEmailTest.class);

    @AfterMethod
    public void tearDown() {
        mailServer.stop();
    }

    @Test(description = "2.12.1.1")
    public void publishEmailsViaSSL () throws IOException, MessagingException, InterruptedException {
        //SiddhiApp creation
        URI workerBaseURI = URI.create(String.format("https://%s:%d", WORKER_IP, WORKER_PORT));
        String path = "/siddhi-apps";
        String contentType = "text/plain";
        String method = "POST";
        String inStreamDefinition = "@App:name('TestSiddhiApp1')" +
                "@source(type='http', @map(type='xml') )" +
                "define stream inputStream (name string, amount double);";

        String outStreamDefinition = "@sink(type='email', @map(type='text') ,"
                + " username ='" + USERNAME + "',"
                + " address ='" + ADDRESS + "',"
                + " password= '" + PASSWORD + "',"
                + " ssl.enable= 'true',"
                + " host= 'localhost',"
                + " port= '3465',"
                + " auth= 'true',"
                + " subject='TestEmailGeneration}' ,"
                + " to='to@localhost',"
                + " cc='cc@localhost',"
                + " bcc='bcc@localhost')"
                + " define stream outputStream (name string, amount double);";

        String query = ("@info(name = 'query') "
                + "from inputStream#log('AA') "
                + "select name, amount "
                + "insert into outputStream;"
        );

        // setup user on the mail server
        Security.setProperty("ssl.SocketFactory.provider", DummySSLSocketFactory.class.getName());
        mailServer = new GreenMail(ServerSetupTest.SMTPS);
        mailServer.start();
        mailServer.setUser(ADDRESS, USERNAME, PASSWORD);

        //Deploy Siddhi App
        HttpTestUtil util = new HttpTestUtil();
        int response = util.httpsRequest(inStreamDefinition + outStreamDefinition + query, workerBaseURI, path,
                true, false, method, contentType, "admin", "admin");
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

        mailServer.waitForIncomingEmail(5000, 3);
        MimeMessage[] messages = mailServer.getReceivedMessages();
        assertTrue(messages.length > 0);
        assertTrue(messages[0].getSubject().contains("TestEmailGeneration"));
        lst.shutdown();
        log.info(messages[0].getContent().toString());
    }

    @Test(description = "2.12.1.2")
    public void changeContentTypeOfHTTPStream () throws IOException, MessagingException, InterruptedException {
        //SiddhiApp creation
        URI workerBaseURI = URI.create(String.format("https://%s:%d", WORKER_IP, WORKER_PORT));
        String path = "/siddhi-apps";
        String contentType = "text/plain";
        String method = "POST";
        String inStreamDefinition = "@App:name('TestSiddhiApp1')" +
                "@source(type='http', @map(type='xml') )" +
                "define stream inputStream (name string, amount double);";

        String outStreamDefinition = "@sink(type='email', content.type='text/html', @map(type='text') ,"
                + " username ='" + USERNAME + "',"
                + " address ='" + ADDRESS + "',"
                + " password= '" + PASSWORD + "',"
                + " ssl.enable= 'true',"
                + " host= 'localhost',"
                + " port= '3465',"
                + " auth= 'true',"
                + " subject='TestEmailGeneration}' ,"
                + " to='to@localhost')"
                + " define stream outputStream (name string, amount double);";

        String query = ("@info(name = 'query') "
                + "from inputStream#log('AA') "
                + "select name, amount "
                + "insert into outputStream;"
        );

        // setup user on the mail server
        Security.setProperty("ssl.SocketFactory.provider", DummySSLSocketFactory.class.getName());
        mailServer = new GreenMail(ServerSetupTest.SMTPS);
        mailServer.start();
        mailServer.setUser(ADDRESS, USERNAME, PASSWORD);
        Thread.sleep(500);

        //Deploy Siddhi App
        HttpTestUtil util = new HttpTestUtil();
        int response = util.httpsRequest(inStreamDefinition + outStreamDefinition + query, workerBaseURI, path,
                true, false, method, contentType, "admin", "admin");
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

        mailServer.waitForIncomingEmail(5000, 3);
        MimeMessage[] messages = mailServer.getReceivedMessages();
        assertTrue(messages.length > 0);
        assertTrue(messages[0].getSubject().contains("TestEmailGeneration"));
        log.info(messages[0].getContent().toString());
        lst.shutdown();
    }

    @Test(description = "2.12.1.3")
    public void CustomSubjectAndBodyTest () throws IOException, MessagingException, InterruptedException {
        //SiddhiApp creation
        URI workerBaseURI = URI.create(String.format("https://%s:%d", WORKER_IP, WORKER_PORT));
        String path = "/siddhi-apps";
        String contentType = "text/plain";
        String method = "POST";
        String inStreamDefinition = "@App:name('TestSiddhiApp1')" +
                "@source(type='http', @map(type='xml') )" +
                "define stream inputStream (name string, amount double);";

        String outStreamDefinition = "@sink(type='email', @map(type='text') ,"
                + " username ='" + USERNAME + "',"
                + " address ='" + ADDRESS + "',"
                + " password= '" + PASSWORD + "',"
                + " ssl.enable= 'true',"
                + " host= 'localhost',"
                + " port= '3465',"
                + " auth= 'true',"
                + " subject='TestEmailGeneration-{{name}}' ,"
                + " to='to@localhost')"
                + " define stream outputStream (name string, amount double);";

        String query = ("@info(name = 'query') "
                + "from inputStream#log('AA') "
                + "select name, amount "
                + "insert into outputStream;"
        );

        // setup user on the mail server
        Security.setProperty("ssl.SocketFactory.provider", DummySSLSocketFactory.class.getName());
        mailServer = new GreenMail(ServerSetupTest.SMTPS);
        mailServer.start();
        mailServer.setUser(ADDRESS, USERNAME, PASSWORD);

        //Deploy Siddhi App
        HttpTestUtil util = new HttpTestUtil();
        int response = util.httpsRequest(inStreamDefinition + outStreamDefinition + query, workerBaseURI, path,
                true, false, method, contentType, "admin", "admin");
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

        mailServer.waitForIncomingEmail(5000, 3);
        MimeMessage[] messages = mailServer.getReceivedMessages();
        assertTrue(messages.length > 0);
        assertTrue(messages[0].getSubject().contains("TestEmailGeneration-John"));
        lst.shutdown();
        log.info(messages[0].getSubject());
    }

    @Test(description = "2.12.1.4")
    public void sendEmailWithAttachmentTest () throws IOException, MessagingException, InterruptedException {
        //SiddhiApp creation
        URI workerBaseURI = URI.create(String.format("https://%s:%d", WORKER_IP, WORKER_PORT));
        String path = "/siddhi-apps";
        String contentType = "text/plain";
        String method = "POST";
        String inStreamDefinition = "@App:name('TestSiddhiApp1')" +
                "@source(type='http', @map(type='xml') )" +
                "define stream inputStream (name string, amount double);";
        /*
        * Attachment path should be an absolute path
        * */

        String outStreamDefinition = "@sink(type='email', @map(type='text') ,"
                + " username ='" + USERNAME + "',"
                + " address ='" + ADDRESS + "',"
                + " password= '" + PASSWORD + "',"
                + " ssl.enable= 'true',"
                + " host= 'localhost',"
                + " port= '3465',"
                + " auth= 'true',"
                + " subject='TestEmailGeneration}' ,"
                + " attachments='/home/lakshan/Desktop/d',"
                + " to='to@localhost')"
                + " define stream outputStream (name string, amount double);";

        String query = ("@info(name = 'query') "
                + "from inputStream#log('AA') "
                + "select name, amount "
                + "insert into outputStream;"
        );

        // setup user on the mail server
        Security.setProperty("ssl.SocketFactory.provider", DummySSLSocketFactory.class.getName());
        mailServer = new GreenMail(ServerSetupTest.SMTPS);
        mailServer.start();
        mailServer.setUser(ADDRESS, USERNAME, PASSWORD);

        //Deploy Siddhi App
        HttpTestUtil util = new HttpTestUtil();
        int response = util.httpsRequest(inStreamDefinition + outStreamDefinition + query, workerBaseURI, path,
                true, false, method, contentType, "admin", "admin");
        Assert.assertEquals(response, 201, "Failed to deploy Siddhi App");
        Thread.sleep(5000);

        //Send events to HTTP source
        URI sourceURI = URI.create(String.format("http://%s:%d", HTTP_SOURCE_IP, HTTP_SOURCE_PORT));
        Random rand = new Random();
        HttpServerListenerHandler lst = new HttpServerListenerHandler(HTTP_SINK_PORT);
        lst.run();
        for (int i = 0; i < 2; i++) {
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

        mailServer.waitForIncomingEmail(5000, 3);
        MimeMessage[] messages = mailServer.getReceivedMessages();
        assertTrue(messages.length > 0);
        assertTrue(messages[0].getSubject().contains("TestEmailGeneration"));
        lst.shutdown();
        // TODO: 28/02/19 Need to assert the attachment
    }


    /*@Test(description = "2.12.1.5")
    public void testGmail () throws IOException, MessagingException, InterruptedException {
        //SiddhiApp creation
        URI workerBaseURI = URI.create(String.format("https://%s:%d", WORKER_IP, WORKER_PORT));
        String path = "/siddhi-apps";
        String contentType = "text/plain";
        String method = "POST";
        String inStreamDefinition = "@App:name('TestSiddhiApp1')" +
                "@source(type='http', @map(type='xml') )" +
                "define stream inputStream (name string, amount double);";

        String outStreamDefinition = "@sink(type='email', @map(type='text') ,"
                + " username ='wso2sptest',"
                + " address ='wso2sptest@gmail.com',"
                + " password= 'wso2carbon123',"
                + " ssl.enable= 'true',"
                + " host= 'smtp.gmail.com',"
                + " port= '465',"
                + " auth= 'true',"
                + " subject='TestEmailGeneration}' ,"
                + " to='lakshan230897@gmail.com',"
                + " attachments='/home/lakshan/Desktop/d')"
                + " define stream outputStream (name string, amount double);";

        String query = ("@info(name = 'query') "
                + "from inputStream#log('AA') "
                + "select name, amount "
                + "insert into outputStream;"
        );

        // setup user on the mail server
        Security.setProperty("ssl.SocketFactory.provider", DummySSLSocketFactory.class.getName());
        mailServer = new GreenMail(ServerSetupTest.SMTPS);
        mailServer.start();
        mailServer.setUser(ADDRESS, USERNAME, PASSWORD);

        //Deploy Siddhi App
        HttpTestUtil util = new HttpTestUtil();
        int response = util.httpsRequest(inStreamDefinition + outStreamDefinition + query, workerBaseURI, path,
                true, false, method, contentType, "admin", "admin");
        Assert.assertEquals(response, 201, "Failed to deploy Siddhi App");
        Thread.sleep(5000);

        //Send events to HTTP source
        URI sourceURI = URI.create(String.format("http://%s:%d", HTTP_SOURCE_IP, HTTP_SOURCE_PORT));
        Random rand = new Random();
        HttpServerListenerHandler lst = new HttpServerListenerHandler(HTTP_SINK_PORT);
        lst.run();
        for (int i = 0; i < 2; i++) {
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

        *//*mailServer.waitForIncomingEmail(5000, 3);
        MimeMessage[] messages = mailServer.getReceivedMessages();
        assertTrue(messages.length > 0);
        assertTrue(messages[0].getSubject().contains("TestEmailGeneration"));*//*
        lst.shutdown();
        //log.info(messages[0].getContent().toString());
    }*/
}
