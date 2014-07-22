package org.infinispan.server.test.security.rest;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.security.cert.X509Certificate;

import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManager;
import javax.servlet.http.HttpServletResponse;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.HttpClientConnectionManager;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.X509HostnameVerifier;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.infinispan.arquillian.core.InfinispanResource;
import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.infinispan.arquillian.core.RunningServer;
import org.infinispan.arquillian.core.WithRunningServer;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.security.JBossJSSESecurityDomain;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

/**
 * Tests CLIENT-CERT security for REST endpoint as is configured via "auth-method" attribute on "rest-connector" element
 * in datagrid subsystem.
 * <p/>
 * In order to configure CLIENT-CERT security, we add a new security-domain in the security subsystem
 * and a new https connector in the web subsystem. This is done via XSL transformations.
 * <p/>
 * Client authenticates himself with client.keystore file. Server contains jsse.keystore file in security subsystem as a
 * truststore and server.keystore file in the web connector as a certificate file. How to create and inspect those files
 * is described e.g. at http://docs.oracle.com/javase/6/docs/technotes/guides/security/jsse/JSSERefGuide.html
 * <p/>
 * Password for all the files is the same: "changeit" The user is allowed to connect to the secured REST endpoint with
 * "test" alias cos the server has this alias registered in its truststore. There's also another alias "test2" which is
 * used to verify that authentication fails - server does not have it in its truststore.
 * <p/>
 * The REST endpoint requires users to be in "REST" role which is defined in roles.properties.
 *
 * @author <a href="mailto:mgencur@redhat.com">Martin Gencur</a>
 */
@RunWith(Arquillian.class)
@Ignore
public class RESTCertSecurityIT {
    private static final String CONTAINER = "rest-security-cert";

    private static final String KEY_A = "a";
    private static final String KEY_B = "b";
    private static final String KEY_C = "c";
    private static final String KEY_D = "d";
    private static final String testAlias = "test";
    private static final String test2Alias = "test2";

    @InfinispanResource("rest-security-cert")
    RemoteInfinispanServer server;

    @Test
    @WithRunningServer({@RunningServer(name = CONTAINER, config = "testsuite/rest-sec-cert-wr.xml")})
    public void testSecuredWriteOperations() throws Exception {
        //correct alias for the certificate
        put(securedClient(testAlias), keyAddress(KEY_A), HttpServletResponse.SC_OK);
        //test wrong authorization, 1. wrong alias for the certificate
        put(securedClient(test2Alias), keyAddress(KEY_B), HttpServletResponse.SC_FORBIDDEN);
        //2. access over 8080
        put(securedClient(testAlias), keyAddressUnsecured(KEY_B), HttpServletResponse.SC_UNAUTHORIZED);
        post(securedClient(testAlias), keyAddress(KEY_C), HttpServletResponse.SC_OK);
        post(securedClient(test2Alias), keyAddress(KEY_D), HttpServletResponse.SC_FORBIDDEN);
        //get is not secured, should be working over 8080
        HttpResponse resp = get(securedClient(test2Alias), keyAddressUnsecured(KEY_A), HttpServletResponse.SC_OK);
        String content = new BufferedReader(new InputStreamReader(resp.getEntity().getContent())).readLine();
        assertEquals("data", content);
        head(securedClient(test2Alias), keyAddressUnsecured(KEY_A), HttpServletResponse.SC_OK);
        delete(securedClient(test2Alias), keyAddress(KEY_A), HttpServletResponse.SC_FORBIDDEN);
        delete(securedClient(testAlias), keyAddress(KEY_A), HttpServletResponse.SC_OK);
        delete(securedClient(testAlias), keyAddress(KEY_C), HttpServletResponse.SC_OK);
    }

    @Test
    @WithRunningServer({@RunningServer(name = CONTAINER, config = "testsuite/rest-sec-cert-rw.xml")})
    public void testSecuredReadWriteOperations() throws Exception {
        //correct alias for the certificate
        put(securedClient(testAlias), keyAddress(KEY_A), HttpServletResponse.SC_OK);
        //test wrong authorization, 1. wrong alias for the certificate
        put(securedClient(test2Alias), keyAddress(KEY_B), HttpServletResponse.SC_FORBIDDEN);
        //2. access over 8080
        put(securedClient(testAlias), keyAddressUnsecured(KEY_B), HttpServletResponse.SC_UNAUTHORIZED);
        post(securedClient(testAlias), keyAddress(KEY_C), HttpServletResponse.SC_OK);
        post(securedClient(test2Alias), keyAddress(KEY_D), HttpServletResponse.SC_FORBIDDEN);
        //get is secured too
        HttpResponse resp = get(securedClient(testAlias), keyAddress(KEY_A), HttpServletResponse.SC_OK);
        String content = new BufferedReader(new InputStreamReader(resp.getEntity().getContent())).readLine();
        assertEquals("data", content);
        //test wrong authorization, 1. wrong alias for the certificate
        get(securedClient(test2Alias), keyAddress(KEY_A), HttpServletResponse.SC_FORBIDDEN);
        //2. access over 8080
        get(securedClient(testAlias), keyAddressUnsecured(KEY_A), HttpServletResponse.SC_UNAUTHORIZED);
        head(securedClient(test2Alias), keyAddress(KEY_A), HttpServletResponse.SC_FORBIDDEN);
        //access over 8080
        head(securedClient(testAlias), keyAddressUnsecured(KEY_A), HttpServletResponse.SC_UNAUTHORIZED);
        head(securedClient(testAlias), keyAddress(KEY_A), HttpServletResponse.SC_OK);
        delete(securedClient(test2Alias), keyAddress(KEY_A), HttpServletResponse.SC_FORBIDDEN);
        delete(securedClient(testAlias), keyAddress(KEY_A), HttpServletResponse.SC_OK);
        delete(securedClient(testAlias), keyAddress(KEY_C), HttpServletResponse.SC_OK);
    }

    private String keyAddress(String key) {
        return "https://" + server.getRESTEndpoint().getInetAddress().getHostName() + ":8443"
                + server.getRESTEndpoint().getContextPath() + "/default/" + key;
    }

    private String keyAddressUnsecured(String key) {
        return "http://" + server.getRESTEndpoint().getInetAddress().getHostName() + ":8080"
                + server.getRESTEndpoint().getContextPath() + "/default/" + key;
    }

    private HttpResponse put(CloseableHttpClient httpClient, String uri, int expectedCode) throws Exception {
        HttpResponse response;
        HttpPut put = new HttpPut(uri);
        put.setEntity(new StringEntity("data", "UTF-8"));
        response = httpClient.execute(put);
        assertEquals(expectedCode, response.getStatusLine().getStatusCode());
        return response;
    }

    private HttpResponse post(CloseableHttpClient httpClient, String uri, int expectedCode) throws Exception {
        HttpResponse response;

        HttpPost post = new HttpPost(uri);
        post.setEntity(new StringEntity("data", "UTF-8"));
        response = httpClient.execute(post);
        assertEquals(expectedCode, response.getStatusLine().getStatusCode());
        return response;
    }

    private HttpResponse get(CloseableHttpClient httpClient, String uri, int expectedCode) throws Exception {
        HttpResponse response;
        HttpGet get = new HttpGet(uri);
        response = httpClient.execute(get);
        assertEquals(expectedCode, response.getStatusLine().getStatusCode());
        return response;
    }

    private HttpResponse delete(CloseableHttpClient httpClient, String uri, int expectedCode) throws Exception {
        HttpResponse response;
        HttpDelete delete = new HttpDelete(uri);
        response = httpClient.execute(delete);
        assertEquals(expectedCode, response.getStatusLine().getStatusCode());
        return response;
    }

    private HttpResponse head(CloseableHttpClient httpClient, String uri, int expectedCode) throws Exception {
        HttpResponse response;
        HttpHead head = new HttpHead(uri);
        response = httpClient.execute(head);
        assertEquals(expectedCode, response.getStatusLine().getStatusCode());
        return response;
    }

    public static CloseableHttpClient securedClient(String alias) throws Exception {
       SSLContext ctx = SSLContext.getInstance("TLS");
       JBossJSSESecurityDomain jsseSecurityDomain = new JBossJSSESecurityDomain("client_cert_auth");
       jsseSecurityDomain.setKeyStorePassword("changeit");
       ClassLoader tccl = Thread.currentThread().getContextClassLoader();
       URL keystore = tccl.getResource("client.keystore");
       jsseSecurityDomain.setKeyStoreURL(keystore.getPath());
       jsseSecurityDomain.setClientAlias(alias);
       jsseSecurityDomain.reloadKeyAndTrustStore();
       KeyManager[] keyManagers = jsseSecurityDomain.getKeyManagers();
       TrustManager[] trustManagers = jsseSecurityDomain.getTrustManagers();
       ctx.init(keyManagers, trustManagers, null);
       X509HostnameVerifier verifier = new X509HostnameVerifier() {

           @Override
           public void verify(String s, SSLSocket sslSocket) throws IOException {
           }

           @Override
           public void verify(String s, X509Certificate x509Certificate) throws SSLException {
           }

           @Override
           public void verify(String s, String[] strings, String[] strings1) throws SSLException {
           }

           @Override
           public boolean verify(String string, SSLSession ssls) {
               return true;
           }
       };
       ConnectionSocketFactory sslssf = new SSLConnectionSocketFactory(ctx, verifier);//SSLSocketFactory.ALLOW_ALL_HOSTNAME_VERIFIER);
       ConnectionSocketFactory plainsf = new PlainConnectionSocketFactory();
       Registry<ConnectionSocketFactory> sr = RegistryBuilder.<ConnectionSocketFactory>create()
               .register("http", plainsf)
               .register("https", sslssf)
               .build();
       HttpClientConnectionManager pcm = new PoolingHttpClientConnectionManager(sr);
       CloseableHttpClient httpClient = HttpClients.custom()
               .setConnectionManager(pcm)
               .build();

       return httpClient;
    }
}
