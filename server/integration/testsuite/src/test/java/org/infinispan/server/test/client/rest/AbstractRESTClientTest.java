package org.infinispan.server.test.client.rest;

import org.apache.http.HttpResponse;
import org.apache.http.util.EntityUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.servlet.http.HttpServletResponse;
import java.io.*;
import java.net.URLEncoder;
import java.util.Arrays;

import static org.infinispan.server.test.client.rest.RESTHelper.*;
import static org.junit.Assert.*;

/**
 * Tests for the REST client. Subclasses must implement the addRestServer, which has to setup the RESTHelper
 * by calling RESTHelper.addServer method.
 *
 * @author <a href="mailto:jvilkola@redhat.com">Jozef Vilkolak</a>
 * @author <a href="mailto:mlinhard@redhat.com">Michal Linhard</a>
 * @version August 2011
 *          <p/>
 *          TODO test for https://issues.jboss.org/browse/ISPN-1193
 */
public abstract class AbstractRESTClientTest {

    protected abstract void addRestServer();

    private static final String DEFAULT_NAMED_CACHE = "namedCache";

    public static class TestSerializable implements Serializable {
        private String content;

        public TestSerializable(String content) {
            super();
            this.content = content;
        }

        public String getContent() {
            return content;
        }
    }

    @Before
    public void setUp() throws Exception {
        addRestServer();

        delete(fullPathKey(KEY_A));
        delete(fullPathKey(KEY_B));
        delete(fullPathKey(KEY_C));
        delete(fullPathKey(DEFAULT_NAMED_CACHE, KEY_A));

        head(fullPathKey(KEY_A), HttpServletResponse.SC_NOT_FOUND);
        head(fullPathKey(KEY_B), HttpServletResponse.SC_NOT_FOUND);
        head(fullPathKey(KEY_C), HttpServletResponse.SC_NOT_FOUND);
        head(fullPathKey(DEFAULT_NAMED_CACHE, KEY_A), HttpServletResponse.SC_NOT_FOUND);
    }

    @After
    public void tearDown() throws Exception {
        delete(fullPathKey(KEY_A));
        delete(fullPathKey(KEY_B));
        delete(fullPathKey(KEY_C));
        delete(fullPathKey(DEFAULT_NAMED_CACHE, KEY_A));
    }

    @Test
    public void testBasicOperation() throws Exception {
        String fullPathKey = fullPathKey(KEY_A);
        String initialXML = "<hey>ho</hey>";

        HttpResponse insert = put(fullPathKey, initialXML, "application/octet-stream");

        assertEquals(0, insert.getEntity().getContentLength());

        HttpResponse get = get(fullPathKey, initialXML);
        assertEquals("application/octet-stream", get.getHeaders("Content-Type")[0].getValue());

        delete(fullPathKey);
        get(fullPathKey, HttpServletResponse.SC_NOT_FOUND);

        put(fullPathKey, initialXML, "application/octet-stream");
        get(fullPathKey, initialXML);

        delete(fullPathKey(null));
        get(fullPathKey, HttpServletResponse.SC_NOT_FOUND);

        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        ObjectOutputStream oo = new ObjectOutputStream(bout);
        oo.writeObject(new TestSerializable("CONTENT"));
        oo.flush();

        byte[] byteData = bout.toByteArray();
        put(fullPathKey, byteData, "application/octet-stream");

        HttpResponse resp = getWithoutClose(fullPathKey);
        int respLength = new Long(resp.getEntity().getContentLength()).intValue();
        byte[] bytesBack = new byte[respLength];
        resp.getEntity().getContent().read(bytesBack, 0, respLength);
        EntityUtils.consume(resp.getEntity());
        assertEquals(byteData.length, bytesBack.length);

        ObjectInputStream oin = new ObjectInputStream(new ByteArrayInputStream(bytesBack));
        TestSerializable ts = (TestSerializable) oin.readObject();
        assertEquals("CONTENT", ts.getContent());
    }

    @Test
    public void testEmptyGet() throws Exception {
        get(fullPathKey("nodata"), HttpServletResponse.SC_NOT_FOUND);
    }

    @Test
    public void testGet() throws Exception {
        String fullPathKey = fullPathKey(KEY_A);
        post(fullPathKey, "data", "application/text");
        HttpResponse resp = get(fullPathKey, "data");
        assertNotNull(resp.getHeaders("ETag")[0].getValue());
        assertNotNull(resp.getHeaders("Last-Modified")[0].getValue());
        assertEquals("application/text", resp.getHeaders("Content-Type")[0].getValue());
    }

    @Test
    public void testGetNamedCache() throws Exception {
        String fullPathKey = fullPathKey(DEFAULT_NAMED_CACHE, KEY_A);
        post(fullPathKey, "data", "application/text");
        HttpResponse resp = get(fullPathKey, "data");
        assertNotNull(resp.getHeaders("ETag")[0].getValue());
        assertNotNull(resp.getHeaders("Last-Modified")[0].getValue());
        assertEquals("application/text", resp.getHeaders("Content-Type")[0].getValue());
    }

    @Test
    public void testHead() throws Exception {
        String fullPathKey = fullPathKey(KEY_A);
        post(fullPathKey, "data", "application/text");
        HttpResponse resp = null;
        try {
            resp = headWithoutClose(fullPathKey);
            assertNotNull(resp.getHeaders("ETag")[0].getValue());
            assertNotNull(resp.getHeaders("Last-Modified")[0].getValue());
            assertEquals("application/text", resp.getHeaders("Content-Type")[0].getValue());
            assertNull(resp.getEntity());
        } finally {
            EntityUtils.consume(resp.getEntity());
        }
    }

    @Test
    public void testPostDuplicate() throws Exception {
        String fullPathKey = fullPathKey(KEY_A);

        post(fullPathKey, "data", "application/text");
        // second post, returns 409
        post(fullPathKey, "data", "application/text", HttpServletResponse.SC_CONFLICT);
        // Should be all ok as its a put
        put(fullPathKey, "data", "application/text");
    }

    @Test
    public void testPutDataWithTimeToLive() throws Exception {
        String fullPathKey = fullPathKey(KEY_A);

        post(fullPathKey, "data", "application/text", HttpServletResponse.SC_OK,
                // headers
                "Content-Type", "application/text", "timeToLiveSeconds", "2");

        get(fullPathKey, "data");
        Thread.sleep(2100);
        // should be evicted
        head(fullPathKey, HttpServletResponse.SC_NOT_FOUND);
    }

    @Test
    public void testPutDataWithMaxIdleTime() throws Exception {
        String fullPathKey = fullPathKey(KEY_A);

        post(fullPathKey, "data", "application/text", HttpServletResponse.SC_OK,
                // headers
                "Content-Type", "application/text", "maxIdleTimeSeconds", "2");

        get(fullPathKey, "data");

        // data is not idle for next 3 seconds
        for (int i = 1; i < 3; i++) {
            Thread.sleep(1000);
            head(fullPathKey);
        }

        // idle for 2 seconds
        Thread.sleep(2100);
        // should be evicted
        head(fullPathKey, HttpServletResponse.SC_NOT_FOUND);
    }

    @Test
    public void testPutDataTTLMaxIdleCombo1() throws Exception {
        String fullPathKey = fullPathKey(KEY_A);

        post(fullPathKey, "data", "application/text", HttpServletResponse.SC_OK,
                // headers
                "Content-Type", "application/text", "timeToLiveSeconds", "10", "maxIdleTimeSeconds", "2");

        get(fullPathKey, "data");

        // data is not idle for next 3 seconds
        for (int i = 1; i < 3; i++) {
            Thread.sleep(1000);
            head(fullPathKey);
        }

        // idle for 2 seconds
        Thread.sleep(2100);
        // should be evicted
        head(fullPathKey, HttpServletResponse.SC_NOT_FOUND);
    }

    @Test
    public void testPutDataTTLMaxIdleCombo2() throws Exception {
        String fullPathKey = fullPathKey(KEY_A);

        post(fullPathKey, "data", "application/text", HttpServletResponse.SC_OK,
                // headers
                "Content-Type", "application/text", "timeToLiveSeconds", "2", "maxIdleTimeSeconds", "10");

        get(fullPathKey, "data");
        Thread.sleep(2100);
        // should be evicted
        head(fullPathKey, HttpServletResponse.SC_NOT_FOUND);
    }

    @Test
    public void testRemoveEntry() throws Exception {
        String fullPathKey = fullPathKey(KEY_A);
        post(fullPathKey, "data", "application/text");
        head(fullPathKey);
        delete(fullPathKey);
        head(fullPathKey, HttpServletResponse.SC_NOT_FOUND);
    }

    @Test
    public void testWipeCacheBucket() throws Exception {
        post(fullPathKey(KEY_A), "data", "application/text");
        post(fullPathKey(KEY_B), "data", "application/text");
        head(fullPathKey(KEY_A));
        head(fullPathKey(KEY_B));
        delete(fullPathKey(null));
        head(fullPathKey(KEY_A), HttpServletResponse.SC_NOT_FOUND);
        head(fullPathKey(KEY_B), HttpServletResponse.SC_NOT_FOUND);
    }

    @Test
    public void testPutUnknownClass() throws Exception {
        String fullPathKey = fullPathKey("x");

        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        ObjectOutputStream oo = new ObjectOutputStream(bout);
        oo.writeObject(new TestSerializable("CONTENT"));
        oo.flush();
        byte[] byteData = bout.toByteArray();
        put(fullPathKey, byteData, "application/x-java-serialized-object");
        HttpResponse resp = get(fullPathKey, null, HttpServletResponse.SC_OK, false, "Accept", "application/x-java-serialized-object");
        int respLength = new Long(resp.getEntity().getContentLength()).intValue();
        byte[] bytesBack = new byte[respLength];
        resp.getEntity().getContent().read(bytesBack, 0, respLength);
        EntityUtils.consume(resp.getEntity());
        assertEquals(byteData.length, bytesBack.length);
        ObjectInputStream oin = new ObjectInputStream(new ByteArrayInputStream(bytesBack));
        TestSerializable ts = (TestSerializable) oin.readObject();
        assertEquals("CONTENT", ts.getContent());
    }

    @Test
    public void testPutKnownClass() throws Exception {
        String fullPathKey = fullPathKey("y");
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        ObjectOutputStream oo = new ObjectOutputStream(bout);
        Integer i1 = 42;
        oo.writeObject(i1);
        oo.flush();
        byte[] byteData = bout.toByteArray();
        put(fullPathKey, byteData, "application/x-java-serialized-object");
        HttpResponse resp = get(fullPathKey, null, HttpServletResponse.SC_OK, false, "Accept", "application/x-java-serialized-object");
        int respLength = new Long(resp.getEntity().getContentLength()).intValue();
        byte[] bytesBack = new byte[respLength];
        resp.getEntity().getContent().read(bytesBack, 0, respLength);
        EntityUtils.consume(resp.getEntity());
        assertEquals(byteData.length, bytesBack.length);
        ObjectInputStream oin = new ObjectInputStream(new ByteArrayInputStream(bytesBack));
        Integer i2 = (Integer) oin.readObject();
        assertEquals(i1, i2);
    }

    @Test
    public void testETagChanges() throws Exception {
        String fullPathKey = fullPathKey(KEY_A);
        put(fullPathKey, "data1", "application/text");
        String eTagFirst = get(fullPathKey).getHeaders("ETag")[0].getValue();
        // second get should get the same ETag
        assertEquals(eTagFirst, get(fullPathKey).getHeaders("ETag")[0].getValue());
        // do second PUT
        put(fullPathKey, "data2", "application/text");
        // get ETag again
        assertFalse(eTagFirst.equals(get(fullPathKey).getHeaders("ETag")[0].getValue()));
    }

    @Test
    public void testXJavaSerializedObjectPutAndDelete() throws Exception {
        //show that "application/text" works for delete
        String fullPathKey1 = fullPathKey("j");
        put(fullPathKey1, "data1", "application/text");
        head(fullPathKey1, HttpServletResponse.SC_OK);
        delete(fullPathKey1);
        head(fullPathKey1, HttpServletResponse.SC_NOT_FOUND);

        String fullPathKey2 = fullPathKey("k");
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        ObjectOutputStream oo = new ObjectOutputStream(bout);
        Integer i1 = 42;
        oo.writeObject(i1);
        oo.flush();
        byte[] byteData = bout.toByteArray();
        put(fullPathKey2, byteData, "application/x-java-serialized-object");
        head(fullPathKey2, HttpServletResponse.SC_OK);
        delete(fullPathKey2);
        head(fullPathKey2, HttpServletResponse.SC_NOT_FOUND);
    }

    @Test
    public void testIfModifiedSince() throws Exception {
        String fullPathKey = fullPathKey(KEY_A);
        put(fullPathKey, "data", "application/text");
        HttpResponse resp = get(fullPathKey);
        String dateLast = resp.getHeaders("Last-Modified")[0].getValue();
        String dateMinus = addDay(dateLast, -1);
        String datePlus = addDay(dateLast, 1);

        get(fullPathKey, "data", HttpServletResponse.SC_OK, true,
                // resource has been modified since
                "If-Modified-Since", dateMinus);

        get(fullPathKey, null, HttpServletResponse.SC_NOT_MODIFIED, true,
                // exact same date as stored one
                "If-Modified-Since", dateLast);

        get(fullPathKey, null, HttpServletResponse.SC_NOT_MODIFIED, true,
                // resource hasn't been modified since
                "If-Modified-Since", datePlus);
    }

    @Test
    public void testIfUnmodifiedSince() throws Exception {
        String fullPathKey = fullPathKey(KEY_A);
        put(fullPathKey, "data", "application/text");

        HttpResponse resp = get(fullPathKey);
        String dateLast = resp.getHeaders("Last-Modified")[0].getValue();
        String dateMinus = addDay(dateLast, -1);
        String datePlus = addDay(dateLast, 1);

        get(fullPathKey, "data", HttpServletResponse.SC_OK, true, "If-Unmodified-Since", dateLast);

        get(fullPathKey, "data", HttpServletResponse.SC_OK, true, "If-Unmodified-Since", datePlus);

        get(fullPathKey, null, HttpServletResponse.SC_PRECONDITION_FAILED, true, "If-Unmodified-Since", dateMinus);
    }

    @Test
    public void testIfNoneMatch() throws Exception {
        String fullPathKey = fullPathKey(KEY_A);
        put(fullPathKey, "data", "application/text");
        HttpResponse resp = get(fullPathKey);
        String eTag = resp.getHeaders("ETag")[0].getValue();

        get(fullPathKey, null, HttpServletResponse.SC_NOT_MODIFIED, true, "If-None-Match", eTag);

        get(fullPathKey, "data", HttpServletResponse.SC_OK, true, "If-None-Match", eTag + "garbage");
    }

    @Test
    public void testIfMatch() throws Exception {
        String fullPathKey = fullPathKey(KEY_A);
        put(fullPathKey, "data", "application/text");
        HttpResponse resp = get(fullPathKey);

        String eTag = resp.getHeaders("ETag")[0].getValue();
        // test GET with If-Match behaviour
        get(fullPathKey, "data", HttpServletResponse.SC_OK, true, "If-Match", eTag);

        get(fullPathKey, null, HttpServletResponse.SC_PRECONDITION_FAILED, true, "If-Match", eTag + "garbage");

        // test HEAD with If-Match behaviour
        head(fullPathKey, HttpServletResponse.SC_OK, new String[][]{{"If-Match", eTag}});
        head(fullPathKey, HttpServletResponse.SC_PRECONDITION_FAILED, new String[][]{{"If-Match", eTag + "garbage"}});
    }

    @Test
    public void testNonExistentCache() throws Exception {
        head(fullPathKey("nonexistentcache", "nodata"), HttpServletResponse.SC_NOT_FOUND);
        get(fullPathKey("nonexistentcache", "nodata"), HttpServletResponse.SC_NOT_FOUND);
    }

    @Test
    public void testByteArrayStorage() throws Exception {
        final String KEY_Z = "z";
        byte[] data = "data".getBytes("UTF-8");

        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        ObjectOutputStream oo = new ObjectOutputStream(bout);
        oo.writeObject(data);
        oo.flush();

        byte[] serializedData = bout.toByteArray();
        put(fullPathKey(0, KEY_Z), serializedData, "application/x-java-serialized-object");

        HttpResponse resp = get(fullPathKey(0, KEY_Z), null, HttpServletResponse.SC_OK, false, "Accept",
                "application/x-java-serialized-object");
        int respLength = new Long(resp.getEntity().getContentLength()).intValue();
        byte[] serializedDataBack = new byte[respLength];
        resp.getEntity().getContent().read(serializedDataBack, 0, respLength);
        EntityUtils.consume(resp.getEntity());
        assertTrue(Arrays.equals(serializedData, serializedDataBack));

        ObjectInputStream oin = new ObjectInputStream(new ByteArrayInputStream(serializedDataBack));
        byte[] dataBack = (byte[]) oin.readObject();
        assertTrue(Arrays.equals(data, dataBack));
    }

    @Test
    public void testStoreBigObject() throws Exception {
        int SIZE = 3000000;
        byte[] bytes = new byte[SIZE];
        for (int i = 0; i < SIZE; i++) {
            bytes[i] = (byte) (i % 10);
        }
        put(fullPathKey("object"), bytes, "application/octet-stream");

        HttpResponse resp = getWithoutClose(fullPathKey("object"));
        InputStream responseStream = resp.getEntity().getContent();
        byte[] response = new byte[SIZE];
        byte data;
        int j = 0;
        while ((data = (byte) responseStream.read()) != -1) {
            response[j] = data;
            j++;
        }

        boolean correct = true;
        for (int i = 0; i < SIZE; i++) {
            if (bytes[i] != response[i]) {
                correct = false;
            }
        }
        EntityUtils.consume(resp.getEntity());
        assertTrue(correct);
    }

    //the following property is passed to the server at startup so that this test passes:
    //-Dorg.apache.tomcat.util.buf.UDecoder.ALLOW_ENCODED_SLASH=true
    @Test
    public void testKeyIncludingSlashURLEncoded() throws Exception {
        String encodedSlashKey = URLEncoder.encode("x/y", "UTF-8");
        post(fullPathKey(encodedSlashKey), "data", "application/text");
        HttpResponse get = get(fullPathKey(encodedSlashKey), "data");
        assertNotNull(get.getHeaders("ETag")[0].getValue());
        assertNotNull(get.getHeaders("Last-Modified")[0].getValue());
        assertEquals("application/text", get.getHeaders("Content-Type")[0].getValue());
    }
}
