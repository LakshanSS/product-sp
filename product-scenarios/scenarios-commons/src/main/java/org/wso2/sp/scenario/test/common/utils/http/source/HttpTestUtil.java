package org.wso2.sp.scenario.test.common.utils.http.source;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import javax.net.ssl.*;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.*;
import java.security.*;
import java.security.cert.CertificateException;

public class HttpTestUtil {
    private static final Logger logger = LoggerFactory.getLogger(HttpTestUtil.class);

    public int httpsRequest(String body, URI baseURI, String path, Boolean auth, Boolean keepAlive, String methodType,
                                 String contentType, String userName, String password) {
        try {
            System.setProperty("javax.net.ssl.trustStore", "../../resources/security/client-truststore.jks");
            //Security.addProvider(new com.sun.net.ssl.internal.ssl.Provider());
            char[] passphrase = "wso2carbon".toCharArray(); //password
            KeyStore keystore = KeyStore.getInstance("JKS");

            keystore.load(new FileInputStream("../../resources/security/client-truststore.jks"), passphrase);
            TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            tmf.init(keystore);
            SSLContext context = SSLContext.getInstance("TLS");
            TrustManager[] trustManagers = tmf.getTrustManagers();
            context.init(null, trustManagers, null);
            SSLSocketFactory sf = context.getSocketFactory();
            URL url = baseURI.resolve(path).toURL();
            HttpsURLConnection httpsCon = (HttpsURLConnection) url.openConnection();
            httpsCon.setSSLSocketFactory(sf);
            httpsCon.setRequestMethod(methodType);
            httpsCon.setRequestProperty("Content-Type", contentType);
            httpsCon.setRequestProperty("HTTP_METHOD", methodType);
            if (auth) {
                try {
                    httpsCon.setRequestProperty("Authorization",
                            "Basic " + java.util.Base64.getEncoder().
                                    encodeToString((userName + ":" + password).getBytes()));
                } catch (Exception e) {
                    logger.error(e.getMessage());
                }
            }
            if (keepAlive) {
                httpsCon.setRequestProperty("Connection", "Keep-Alive");
            }
            if (methodType.equals("POST") || methodType.equals("PUT")) {
                httpsCon.setDoOutput(true);
                OutputStreamWriter out = new OutputStreamWriter(httpsCon.getOutputStream());
                out.write(body);
                out.close();
            }
            logger.info("Event response code " + httpsCon.getResponseCode());
            logger.info("Event response message " + httpsCon.getResponseMessage());
            httpsCon.disconnect();
            return httpsCon.getResponseCode();
        } catch (NoSuchAlgorithmException e) {
            logger.error("No such algorithm in while writing output in test server connector ", e);
        } catch (CertificateException e) {
            logger.error("Certificate exception in basic authentication", e);
        } catch (KeyStoreException e) {
            logger.error("Keystore exception in while trying to sent test request ", e);
        } catch (IOException e) {
             logger.error("IOException while trying to send test request ", e);
        } catch (KeyManagementException e) {
            e.printStackTrace();
        }
        return 400;
    }

    public int httpRequest(String body, URI baseURI, String path, Boolean auth, Boolean keepAlive, String methodType,
                           String contentType, String userName, String password) {

        URL url = null;
        try {
            url = baseURI.resolve(path).toURL();
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod(methodType);
            conn.setRequestProperty("HTTP_METHOD", "POST");
            conn.setRequestProperty("Content-Type", contentType);

            if (keepAlive) {
                conn.setRequestProperty("Connection", "Keep-Alive");
            }
            if (auth) {
                conn.setRequestProperty("Authorization",
                        "Basic " + java.util.Base64.getEncoder().encodeToString((userName + ":" + password).getBytes()));
            }
            if (methodType.equals("POST") || methodType.equals("PUT")) {
                conn.setDoOutput(true);
                OutputStreamWriter out = new OutputStreamWriter(conn.getOutputStream());
                out.write(body);
                out.close();
            }
            logger.info("Event response code " + conn.getResponseCode());
            logger.info("Event response message " + conn.getResponseMessage());
            return conn.getResponseCode();
        } catch (MalformedURLException e) {
            e.printStackTrace();
        } catch (ProtocolException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 400;
    }
}
