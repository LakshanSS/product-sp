package org.wso2.sp.scenario.test.common.utils.http.sink;

import com.sun.net.httpserver.HttpServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;

public class HttpServerListenerHandler implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(HttpServerListenerHandler.class);
    private HttpServerListener serverListener;
    private HttpServer server;
    private int port;

    public HttpServerListenerHandler(int port) {
        this.serverListener = new HttpServerListener();
        this.port = port;
    }

    @Override
    public void run() {
        try {
            server = HttpServer.create(new InetSocketAddress(port), 5);
            server.createContext("/abc", serverListener);
            server.start();
        } catch (IOException e) {
            logger.error("Error in creating test server.", e);
        }
    }

    public void shutdown() {
        if (server != null) {
            logger.info("Shutting down");
            server.stop(1);
        }
    }

    public HttpServerListener getServerListener() {
        return serverListener;
    }
}
