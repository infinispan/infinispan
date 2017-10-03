package org.infinispan.server.test.client.rest;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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
import org.apache.http.conn.ssl.NoopHostnameVerifier;
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

    public static final String DEFAULT_CACHE = "default";
    private final String cache;

    public RESTHelper(String cache) {
        this.cache = cache;
    }

    public RESTHelper() {
        cache = DEFAULT_CACHE;
    }

    private int port = 8080;
    private List<Server> servers = new ArrayList<Server>();
    private CredentialsProvider credsProvider = new BasicCredentialsProvider();
    public CloseableHttpClient client = HttpClients.custom().setDefaultCredentialsProvider(credsProvider).build();

    public void addServer(String hostname, String restServerPath) {
        servers.add(new Server(hostname, restServerPath));
    }

    public void clearServers() {
        servers.clear();
    }

    public void addServer(String hostname, int port, String restServerPath) {
        this.port = port;
        servers.add(new Server(hostname, restServerPath));
    }

    public static String addDay(String aDate, int days) throws Exception {
        ZonedDateTime date = ZonedDateTime.from(DateTimeFormatter.RFC_1123_DATE_TIME.parse(aDate));
        date = date.plusDays(days);
        return DateTimeFormatter.RFC_1123_DATE_TIME.format(date);
    }

    public HttpResponse head(URI uri) throws Exception {
        return head(uri, HttpStatus.SC_OK);
    }

    public HttpResponse headWithoutClose(URI uri) throws Exception {
        return head(uri, HttpStatus.SC_OK);
    }

    public HttpResponse head(URI uri, int expectedCode) throws Exception {
        return head(uri, expectedCode, new String[0][0]);
    }

    public HttpResponse headWithoutClose(URI uri, int expectedCode) throws Exception {
        return head(uri, expectedCode, new String[0][0]);
    }

    public HttpResponse headWithout(URI uri, int expectedCode, String[][] headers) throws Exception {
        HttpHead head = new HttpHead(uri);
        for (String[] eachHeader : headers) {
            head.setHeader(eachHeader[0], eachHeader[1]);
        }
        HttpResponse resp = client.execute(head);
        assertEquals(expectedCode, resp.getStatusLine().getStatusCode());
        return resp;
    }

    public HttpResponse head(URI uri, int expectedCode, String[][] headers) throws Exception {
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

    public HttpResponse get(URI uri) throws Exception {
        return get(uri, HttpStatus.SC_OK);
    }

    public HttpResponse getWithoutClose(URI uri) throws Exception {
        return getWithoutClose(uri, HttpStatus.SC_OK);
    }

    public HttpResponse get(URI uri, String expectedResponseBody) throws Exception {
        return get(uri, expectedResponseBody, HttpStatus.SC_OK, true);
    }

    public HttpResponse getWithoutClose(URI uri, String expectedResponseBody) throws Exception {
        return get(uri, expectedResponseBody, HttpStatus.SC_OK, false);
    }

    public HttpResponse get(URI uri, int expectedCode) throws Exception {
        return get(uri, null, expectedCode, true, new Object[0]);
    }

    public HttpResponse getWithoutClose(URI uri, int expectedCode) throws Exception {
        return get(uri, null, expectedCode, false, new Object[0]);
    }

    public HttpResponse get(URI uri, String expectedResponseBody, int expectedCode, boolean closeConnection, Object... headers) throws Exception {
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
    public boolean getWithoutAssert(URI uri, String expectedResponseBody, int expectedCode, boolean closeConnection, Object... headers) throws Exception {
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

    public HttpResponse put(URI uri, Object data, String contentType) throws Exception {
        return put(uri, data, contentType, HttpStatus.SC_OK);
    }

    public HttpResponse put(URI uri, Object data, String contentType, int expectedCode) throws Exception {
        return put(uri, data, contentType, expectedCode, new Object[0]);
    }

    public HttpResponse put(URI uri, Object data, String contentType, int expectedCode, Object... headers)
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
        assertEquals("URI=" + uri, expectedCode, resp.getStatusLine().getStatusCode());
        return resp;
    }

    public void setCredentials(String username, String password) {
        Credentials credentials = new UsernamePasswordCredentials(username, password);
        credsProvider.setCredentials(AuthScope.ANY, credentials);
    }

    public void setSni(SSLContext sslContext, java.util.Optional<String> sniHostName) {
        client = HttpClients.custom().setSSLSocketFactory(new SSLConnectionSocketFactory(sslContext, NoopHostnameVerifier.INSTANCE) {
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

    public void clearCredentials() {
        credsProvider.clear();
    }

    public HttpResponse post(URI uri, Object data, String contentType) throws Exception {
        return post(uri, data, contentType, HttpStatus.SC_OK);
    }

    public HttpResponse post(URI uri, Object data, String contentType, int expectedCode) throws Exception {
        return post(uri, data, contentType, expectedCode, new Object[0]);
    }

    public HttpResponse post(URI uri, Object data, String contentType, int expectedCode, Object... headers)
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

    public HttpResponse delete(URI uri) throws Exception {
        HttpDelete delete = new HttpDelete(uri);
        HttpResponse resp = client.execute(delete);
        EntityUtils.consume(resp.getEntity());
        return resp;
    }

    public HttpResponse delete(URI uri, int expectedCode, Object... headers) throws Exception {
        HttpDelete delete = new HttpDelete(uri);
        if (headers.length % 2 != 0)
            throw new IllegalArgumentException("bad headers argument");
        for (int i = 0; i < headers.length; i += 2) {
            delete.setHeader((String) headers[i], (String) headers[i + 1]);
        }
        HttpResponse resp = client.execute(delete);
        EntityUtils.consume(resp.getEntity());
        assertEquals("URI=" + uri, expectedCode, resp.getStatusLine().getStatusCode());
        return resp;
    }

    /**
     * returns full uri for given server number cache name and key if key is null the key part is ommited
     */
    public URI fullPathKey(int server, String cache, String key, int offset) {
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

    public URI toSsl(URI uri) {
        try {
            return new URI(uri.toString().replaceFirst("http", "https"));
        } catch (URISyntaxException e) {
            throw new AssertionError("Not a valid URI", e);
        }
    }

    public URI fullPathKey(int server, String key) {
        return fullPathKey(server, cache, key, 0);
    }

    public URI fullPathKey(int server, String key, int portOffset) {
        return fullPathKey(server, cache, key, portOffset);
    }

    public URI fullPathKey(String key) {
        return fullPathKey(0, key);
    }

    public URI fullPathKey(String cache, String key) {
        return fullPathKey(0, cache, key, 0);
    }

    public URI fullPathKey(String cache, String key, int portOffset) {
        return fullPathKey(0, cache, key, portOffset);
    }

    public List<Server> getServers() {
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
