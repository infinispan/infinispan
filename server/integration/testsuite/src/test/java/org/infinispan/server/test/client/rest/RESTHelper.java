package org.infinispan.server.test.client.rest;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Locale;

import javax.net.ssl.SNIHostName;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocket;

import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.Credentials;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;


/**
 * Utility class.
 *
 * @author <a href="mailto:mlinhard@redhat.com">Michal Linhard</a>
 * @author <a href="mailto:mgencur@redhat.com">Martin Gencur</a>
 */
public class RESTHelper {

    public static final String KEY_A = "a";
    public static final String KEY_B = "b";
    public static final String KEY_C = "c";

    private static final String DATE_PATTERN_RFC1123 = "EEE, dd MMM yyyy HH:mm:ss zzz";

    private static int port = 8080;
    private static List<Server> servers = new ArrayList<Server>();
    private static CredentialsProvider credsProvider = new BasicCredentialsProvider();
    public static CloseableHttpClient client = HttpClients.custom().setDefaultCredentialsProvider(credsProvider).build();


    public static void addServer(String hostname, String restServerPath) {
        servers.add(new Server(hostname, restServerPath));
    }

    public static void clearServers() {
        servers.clear();
    }

    public static void addServer(String hostname, int port, String restServerPath) {
        RESTHelper.port = port;
        servers.add(new Server(hostname, restServerPath));
    }

    public static String addDay(String aDate, int days) throws Exception {
        SimpleDateFormat format = new SimpleDateFormat(DATE_PATTERN_RFC1123, Locale.US);
        Date date = format.parse(aDate);
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        cal.add(Calendar.DATE, days);
        return format.format(cal.getTime());
    }

    public static HttpResponse head(URI uri) throws Exception {
        return head(uri, HttpStatus.SC_OK);
    }

    public static HttpResponse headWithoutClose(URI uri) throws Exception {
        return head(uri, HttpStatus.SC_OK);
    }

    public static HttpResponse head(URI uri, int expectedCode) throws Exception {
        return head(uri, expectedCode, new String[0][0]);
    }

    public static HttpResponse headWithoutClose(URI uri, int expectedCode) throws Exception {
        return head(uri, expectedCode, new String[0][0]);
    }

    public static HttpResponse headWithout(URI uri, int expectedCode, String[][] headers) throws Exception {
        HttpHead head = new HttpHead(uri);
        for (String[] eachHeader : headers) {
            head.setHeader(eachHeader[0], eachHeader[1]);
        }
        HttpResponse resp = client.execute(head);
        assertEquals(expectedCode, resp.getStatusLine().getStatusCode());
        return resp;
    }

    public static HttpResponse head(URI uri, int expectedCode, String[][] headers) throws Exception {
        HttpHead head = new HttpHead(uri);
        HttpResponse resp = null;
        try {
            for (String[] eachHeader : headers) {
                head.setHeader(eachHeader[0], eachHeader[1]);
            }
            resp = client.execute(head);
        } finally {
            EntityUtils.consume(resp.getEntity());
        }
        assertEquals(expectedCode, resp.getStatusLine().getStatusCode());
        return resp;
    }

    public static HttpResponse get(URI uri) throws Exception {
        return get(uri, HttpStatus.SC_OK);
    }

    public static HttpResponse getWithoutClose(URI uri) throws Exception {
        return getWithoutClose(uri, HttpStatus.SC_OK);
    }

    public static HttpResponse get(URI uri, String expectedResponseBody) throws Exception {
        return get(uri, expectedResponseBody, HttpStatus.SC_OK, true);
    }

    public static HttpResponse getWithoutClose(URI uri, String expectedResponseBody) throws Exception {
        return get(uri, expectedResponseBody, HttpStatus.SC_OK, false);
    }

    public static HttpResponse get(URI uri, int expectedCode) throws Exception {
        return get(uri, null, expectedCode, true, new Object[0]);
    }

    public static HttpResponse getWithoutClose(URI uri, int expectedCode) throws Exception {
        return get(uri, null, expectedCode, false, new Object[0]);
    }

    public static HttpResponse get(URI uri, String expectedResponseBody, int expectedCode, boolean closeConnection, Object... headers) throws Exception {
        HttpGet get = new HttpGet(uri);
        if (headers.length % 2 != 0)
            throw new IllegalArgumentException("bad headers argument");
        for (int i = 0; i < headers.length; i += 2) {
            get.setHeader((String) headers[i], (String) headers[i + 1]);
        }
        HttpResponse resp = client.execute(get);
        try {
            assertEquals(uri.toString(), expectedCode, resp.getStatusLine().getStatusCode());
            if (expectedResponseBody != null) {
                assertEquals(expectedResponseBody, EntityUtils.toString(resp.getEntity()));
            }
        } finally {
            if (closeConnection) {
                EntityUtils.consume(resp.getEntity());
            }
        }
        return resp;
    }

    // the same as the normal get, without the asserts, so the caller can decide what to do in case the request fails
    public static boolean getWithoutAssert(URI uri, String expectedResponseBody, int expectedCode, boolean closeConnection, Object... headers) throws Exception {
        HttpGet get = new HttpGet(uri);
        if (headers.length % 2 != 0)
            throw new IllegalArgumentException("bad headers argument");
        for (int i = 0; i < headers.length; i += 2) {
            get.setHeader((String) headers[i], (String) headers[i + 1]);
        }
        HttpResponse resp = client.execute(get);
        try {
            if (expectedCode != resp.getStatusLine().getStatusCode()) {
                return false;
            }
            if (expectedResponseBody != null) {
                if (!expectedResponseBody.equals(EntityUtils.toString(resp.getEntity()))) {
                    return false;
                }
            }
        } finally {
            if (closeConnection) {
                EntityUtils.consume(resp.getEntity());
            }
        }
        return true;
    }

    public static HttpResponse put(URI uri, Object data, String contentType) throws Exception {
        return put(uri, data, contentType, HttpStatus.SC_OK);
    }

    public static HttpResponse put(URI uri, Object data, String contentType, int expectedCode) throws Exception {
        return put(uri, data, contentType, expectedCode, new Object[0]);
    }

    public static HttpResponse put(URI uri, Object data, String contentType, int expectedCode, Object... headers)
            throws Exception {
        HttpPut put = new HttpPut(uri);
        if (data instanceof String) {
            put.setEntity(new StringEntity((String) data, "UTF-8"));
        } else if (data instanceof byte[]) {
            byte[] byteData = (byte[]) data;
            ByteArrayInputStream bs = new ByteArrayInputStream(byteData);
            put.setEntity(new InputStreamEntity(bs, byteData.length));
        } else {
            throw new IllegalArgumentException("Unknown data type for PUT method");
        }
        put.setHeader("Content-Type", contentType);
        if (headers.length % 2 != 0)
            throw new IllegalArgumentException("bad headers argument");
        for (int i = 0; i < headers.length; i += 2) {
            put.setHeader((String) headers[i], (String) headers[i + 1]);
        }
        HttpResponse resp = client.execute(put);
        EntityUtils.consume(resp.getEntity());
        assertEquals(expectedCode, resp.getStatusLine().getStatusCode());
        return resp;
    }

    public static void setCredentials(String username, String password) {
        Credentials credentials = new UsernamePasswordCredentials(username, password);
        credsProvider.setCredentials(
                new AuthScope(servers.get(0).getHostname(), port), credentials);
    }

    public static void setSni(SSLContext sslContext, java.util.Optional<String> sniHostName) {
        client = HttpClients.custom().setSSLSocketFactory(new SSLConnectionSocketFactory(sslContext, SSLConnectionSocketFactory.ALLOW_ALL_HOSTNAME_VERIFIER) {
            @Override
            protected void prepareSocket(SSLSocket socket) throws IOException {
                if(sniHostName.isPresent()) {
                SSLParameters sslParameters = socket.getSSLParameters();
                sslParameters.setServerNames(Arrays.asList(new SNIHostName(sniHostName.get())));
                socket.setSSLParameters(sslParameters);
                }
            }
        }).build();
    }

    public static void clearCredentials() {
        credsProvider.clear();
    }

    public static HttpResponse post(URI uri, Object data, String contentType) throws Exception {
        return post(uri, data, contentType, HttpStatus.SC_OK);
    }

    public static HttpResponse post(URI uri, Object data, String contentType, int expectedCode) throws Exception {
        return post(uri, data, contentType, expectedCode, new Object[0]);
    }

    public static HttpResponse post(URI uri, Object data, String contentType, int expectedCode, Object... headers)
            throws Exception {
        HttpPost post = new HttpPost(uri);
        if (data instanceof String) {
            post.setEntity(new StringEntity((String) data, "UTF-8"));
        } else if (data instanceof byte[]) {
            byte[] byteData = (byte[]) data;
            ByteArrayInputStream bs = new ByteArrayInputStream(byteData);
            post.setEntity(new InputStreamEntity(bs, byteData.length));
        } else {
            throw new IllegalArgumentException("Unknown data type for POST method");
        }
        post.setHeader("Content-Type", contentType);
        if (headers.length % 2 != 0)
            throw new IllegalArgumentException("bad headers argument");
        for (int i = 0; i < headers.length; i += 2) {
            post.setHeader((String) headers[i], (String) headers[i + 1]);
        }
        HttpResponse resp = client.execute(post);
        EntityUtils.consume(resp.getEntity());
        int statusCode = resp.getStatusLine().getStatusCode();
        assertEquals("URI=" + uri, expectedCode, statusCode);
        return resp;
    }

    public static HttpResponse delete(URI uri) throws Exception {
        HttpDelete delete = new HttpDelete(uri);
        HttpResponse resp = client.execute(delete);
        EntityUtils.consume(resp.getEntity());
        return resp;
    }

    public static HttpResponse delete(URI uri, int expectedCode, Object... headers) throws Exception {
        HttpDelete delete = new HttpDelete(uri);
        if (headers.length % 2 != 0)
            throw new IllegalArgumentException("bad headers argument");
        for (int i = 0; i < headers.length; i += 2) {
            delete.setHeader((String) headers[i], (String) headers[i + 1]);
        }
        HttpResponse resp = client.execute(delete);
        EntityUtils.consume(resp.getEntity());
        assertEquals(expectedCode, resp.getStatusLine().getStatusCode());
        return resp;
    }

    /**
     * returns full uri for given server number cache name and key if key is null the key part is ommited
     */
    public static URI fullPathKey(int server, String cache, String key, int offset) {
        final StringBuilder queryBuilder = new StringBuilder(servers.get(server).getRestServerPath() + "/" + cache);
        if (key != null) {
            queryBuilder.append("/").append(key);
        }
        final String query = queryBuilder.toString();
        final String hostname = servers.get(server).getHostname();
        final int connectionPort = port + offset;

        try {
            final URL url = new URL("http", hostname, connectionPort, query);
            try {
                return new URI(url.getProtocol(), url.getUserInfo(), url.getHost(), url.getPort(), url.getPath(), url.getQuery(), null);
            } catch (URISyntaxException e) {
                throw new RuntimeException(e);
            }
        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }
    }

    public static URI toSsl(URI uri) {
        try {
            return new URI(uri.toString().replaceFirst("http", "https"));
        } catch (URISyntaxException e) {
            throw new AssertionError("Not a valid URI", e);
        }
    }

    public static URI fullPathKey(int server, String key) {
        return fullPathKey(server, "___defaultcache", key, 0);
    }

    public static URI fullPathKey(int server, String key, int portOffset) {
        return fullPathKey(server, "___defaultcache", key, portOffset);
    }

    public static URI fullPathKey(String key) {
        return fullPathKey(0, key);
    }

    public static URI fullPathKey(String cache, String key) {
        return fullPathKey(0, cache, key, 0);
    }

    public static URI fullPathKey(String cache, String key, int portOffset) {
        return fullPathKey(0, cache, key, portOffset);
    }

    public static List<Server> getServers() {
        return servers;
    }

    public static class Server {
        private String hostname;
        private String restServerPath;

        public Server(String hostname, String restServerPath) {
            this.hostname = hostname;
            this.restServerPath = restServerPath;
        }

        public String getHostname() {
            return hostname;
        }

        public String getRestServerPath() {
            return restServerPath;
        }
    }
}
