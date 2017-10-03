package org.infinispan.server.test.client.rest;

import static org.infinispan.server.test.client.rest.RESTHelper.KEY_A;
import static org.infinispan.server.test.client.rest.RESTHelper.KEY_B;
import static org.infinispan.server.test.client.rest.RESTHelper.KEY_C;
import static org.infinispan.server.test.client.rest.RESTHelper.addDay;
import static org.infinispan.server.test.util.ITestUtils.sleepForSecs;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.URI;
import java.net.URLEncoder;
import java.util.Arrays;

import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.util.EntityUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for the REST client. Subclasses must implement the addRestServer, which has to setup the RESTHelper
 * by calling RESTHelper.addServer method.
 *
 * @author <a href="mailto:jvilkola@redhat.com">Jozef Vilkolak</a>
 * @author <a href="mailto:mlinhard@redhat.com">Michal Linhard</a>
 */
public abstract class AbstractRESTClientIT {

    protected abstract void addRestServer();

    protected RESTHelper rest;

    protected static final String REST_NAMED_CACHE = "restNamedCache";

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

    protected String getDefaultCache() {
        return "default";
    }

    @Before
    public void setUp() throws Exception {
        rest = new RESTHelper(getDefaultCache());
        addRestServer();

        cleanUpEntries();

        rest.head(rest.fullPathKey(KEY_A), HttpStatus.SC_NOT_FOUND);
        rest.head(rest.fullPathKey(KEY_B), HttpStatus.SC_NOT_FOUND);
        rest.head(rest.fullPathKey(KEY_C), HttpStatus.SC_NOT_FOUND);
        rest.head(rest.fullPathKey(REST_NAMED_CACHE, KEY_A), HttpStatus.SC_NOT_FOUND);
    }

    private void cleanUpEntries() throws Exception {
        rest.delete(rest.fullPathKey(KEY_A));
        rest.delete(rest.fullPathKey(KEY_B));
        rest.delete(rest.fullPathKey(KEY_C));
        rest.delete(rest.fullPathKey(REST_NAMED_CACHE, KEY_A));
    }

    @After
    public void tearDown() throws Exception {
        cleanUpEntries();
        rest.clearServers();
    }

    @Test
    public void testBasicOperation() throws Exception {
        URI fullPathKey = rest.fullPathKey(KEY_A);
        String initialXML = "<hey>ho</hey>";

        HttpResponse insert = rest.put(fullPathKey, initialXML, "application/octet-stream");

        assertEquals(0, insert.getEntity().getContentLength());

        HttpResponse get = rest.get(fullPathKey, initialXML);
        assertEquals("application/octet-stream", get.getHeaders("Content-Type")[0].getValue());

        rest.delete(fullPathKey);
        rest.get(fullPathKey, HttpStatus.SC_NOT_FOUND);

        rest.put(fullPathKey, initialXML, "application/octet-stream");
        rest.get(fullPathKey, initialXML);

        rest.delete(rest.fullPathKey(null));
        rest.get(fullPathKey, HttpStatus.SC_NOT_FOUND);

        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        ObjectOutputStream oo = new ObjectOutputStream(bout);
        oo.writeObject(new TestSerializable("CONTENT"));
        oo.flush();

        byte[] byteData = bout.toByteArray();
        rest.put(fullPathKey, byteData, "application/octet-stream");

        HttpResponse resp = rest.getWithoutClose(fullPathKey);
        ObjectInputStream oin = new ObjectInputStream(resp.getEntity().getContent());
        TestSerializable ts = (TestSerializable) oin.readObject();
        EntityUtils.consume(resp.getEntity());
        assertEquals("CONTENT", ts.getContent());
    }

    @Test
    public void testEmptyGet() throws Exception {
        rest.get(rest.fullPathKey("nodata"), HttpStatus.SC_NOT_FOUND);
    }

    @Test
    public void testGet() throws Exception {
        URI fullPathKey = rest.fullPathKey(KEY_A);
        rest.post(fullPathKey, "data", "application/text");
        HttpResponse resp = rest.get(fullPathKey, "data");
        assertNotNull(resp.getHeaders("ETag")[0].getValue());
        assertNotNull(resp.getHeaders("Last-Modified")[0].getValue());
        assertEquals("application/text;charset=UTF-8", resp.getHeaders("Content-Type")[0].getValue());
    }

    @Test
    public void testGetNamedCache() throws Exception {
        URI fullPathKey = rest.fullPathKey(REST_NAMED_CACHE, KEY_A);
        rest.post(fullPathKey, "data", "application/text");
        HttpResponse resp = rest.get(fullPathKey, "data");
        assertNotNull(resp.getHeaders("ETag")[0].getValue());
        assertNotNull(resp.getHeaders("Last-Modified")[0].getValue());
        assertEquals("application/text;charset=UTF-8", resp.getHeaders("Content-Type")[0].getValue());
    }

    @Test
    public void testHead() throws Exception {
        URI fullPathKey = rest.fullPathKey(KEY_A);
        rest.post(fullPathKey, "data", "application/text");
        HttpResponse resp = null;
        try {
            resp = rest.headWithoutClose(fullPathKey);
            assertNotNull(resp.getHeaders("ETag")[0].getValue());
            assertNotNull(resp.getHeaders("Last-Modified")[0].getValue());
            assertEquals("application/text;charset=UTF-8", resp.getHeaders("Content-Type")[0].getValue());
            assertNull(resp.getEntity());
        } finally {
            EntityUtils.consume(resp.getEntity());
        }
    }

    @Test
    public void testPostDuplicate() throws Exception {
        URI fullPathKey = rest.fullPathKey(KEY_A);

        rest.post(fullPathKey, "data", "application/text");
        // second post, returns 409
        rest.post(fullPathKey, "data", "application/text", HttpStatus.SC_CONFLICT);
        // Should be all ok as its a put
        rest.put(fullPathKey, "data", "application/text");
    }

    @Test
    public void testPutDataWithTimeToLive() throws Exception {
        URI fullPathKey = rest.fullPathKey(KEY_A);

        rest.post(fullPathKey, "data", "application/text", HttpStatus.SC_OK,
                // headers
                "Content-Type", "application/text", "timeToLiveSeconds", "2");

        rest.get(fullPathKey, "data");
        sleepForSecs(2.1);
        // should be evicted
        rest.head(fullPathKey, HttpStatus.SC_NOT_FOUND);
    }

    @Test
    public void testPutDataWithMaxIdleTime() throws Exception {
        URI fullPathKey = rest.fullPathKey(KEY_A);

        rest.post(fullPathKey, "data", "application/text", HttpStatus.SC_OK,
                // headers
                "Content-Type", "application/text", "maxIdleTimeSeconds", "2");

        rest.get(fullPathKey, "data");

        // data is not idle for next 3 seconds
        for (int i = 1; i < 3; i++) {
            sleepForSecs(1);
            rest.head(fullPathKey);
        }

        // idle for 2 seconds
        sleepForSecs(2.1);
        // should be evicted
        rest.head(fullPathKey, HttpStatus.SC_NOT_FOUND);
    }

    @Test
    public void testPutDataTTLMaxIdleCombo1() throws Exception {
        URI fullPathKey = rest.fullPathKey(KEY_A);

        rest.post(fullPathKey, "data", "application/text", HttpStatus.SC_OK,
                // headers
                "Content-Type", "application/text", "timeToLiveSeconds", "10", "maxIdleTimeSeconds", "2");

        rest.get(fullPathKey, "data");

        // data is not idle for next 3 seconds
        for (int i = 1; i < 3; i++) {
            sleepForSecs(1);
            rest.head(fullPathKey);
        }

        // idle for 2 seconds
        sleepForSecs(2.1);
        // should be evicted
        rest.head(fullPathKey, HttpStatus.SC_NOT_FOUND);
    }

    @Test
    public void testPutDataTTLMaxIdleCombo2() throws Exception {
        URI fullPathKey = rest.fullPathKey(KEY_A);

        rest.post(fullPathKey, "data", "application/text", HttpStatus.SC_OK,
                // headers
                "Content-Type", "application/text", "timeToLiveSeconds", "2", "maxIdleTimeSeconds", "10");

        rest.get(fullPathKey, "data");
        sleepForSecs(2.1);
        // should be evicted
        rest.head(fullPathKey, HttpStatus.SC_NOT_FOUND);
    }

    @Test
    public void testRemoveEntry() throws Exception {
        URI fullPathKey = rest.fullPathKey(KEY_A);
        rest.post(fullPathKey, "data", "application/text");
        rest.head(fullPathKey);
        rest.delete(fullPathKey);
        rest.head(fullPathKey, HttpStatus.SC_NOT_FOUND);
    }

    @Test
    public void testWipeCacheBucket() throws Exception {
        rest.post(rest.fullPathKey(KEY_A), "data", "application/text");
        rest.post(rest.fullPathKey(KEY_B), "data", "application/text");
        rest.head(rest.fullPathKey(KEY_A));
        rest.head(rest.fullPathKey(KEY_B));
        rest.delete(rest.fullPathKey(null));
        rest.head(rest.fullPathKey(KEY_A), HttpStatus.SC_NOT_FOUND);
        rest.head(rest.fullPathKey(KEY_B), HttpStatus.SC_NOT_FOUND);
    }

    @Test
    public void testPutUnknownClass() throws Exception {
        URI fullPathKey = rest.fullPathKey("x");

        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        ObjectOutputStream oo = new ObjectOutputStream(bout);
        oo.writeObject(new TestSerializable("CONTENT"));
        oo.flush();
        byte[] byteData = bout.toByteArray();
        rest.put(fullPathKey, byteData, "application/x-java-serialized-object");
        HttpResponse resp = rest.get(fullPathKey, null, HttpStatus.SC_OK, false, "Accept", "application/x-java-serialized-object");
        ObjectInputStream oin = new ObjectInputStream(resp.getEntity().getContent());
        TestSerializable ts = (TestSerializable) oin.readObject();
        EntityUtils.consume(resp.getEntity());
        assertEquals("CONTENT", ts.getContent());
    }

    @Test
    public void testPutKnownClass() throws Exception {
        URI fullPathKey = rest.fullPathKey("y");
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        ObjectOutputStream oo = new ObjectOutputStream(bout);
        Integer i1 = 42;
        oo.writeObject(i1);
        oo.flush();
        byte[] byteData = bout.toByteArray();
        rest.put(fullPathKey, byteData, "application/x-java-serialized-object");
        HttpResponse resp = rest.get(fullPathKey, null, HttpStatus.SC_OK, false, "Accept", "application/x-java-serialized-object");
        ObjectInputStream oin = new ObjectInputStream(resp.getEntity().getContent());
        Integer i2 = (Integer) oin.readObject();
        EntityUtils.consume(resp.getEntity());
        assertEquals(i1, i2);
    }

    @Test
    public void testETagChanges() throws Exception {
        URI fullPathKey = rest.fullPathKey(KEY_A);
        rest.put(fullPathKey, "data1", "application/text");
        String eTagFirst = rest.get(fullPathKey).getHeaders("ETag")[0].getValue();
        // second get should get the same ETag
        assertEquals(eTagFirst, rest.get(fullPathKey).getHeaders("ETag")[0].getValue());
        // do second PUT
        rest.put(fullPathKey, "data2", "application/text");
        // get ETag again
        assertFalse(eTagFirst.equals(rest.get(fullPathKey).getHeaders("ETag")[0].getValue()));
    }

    @Test
    public void testXJavaSerializedObjectPutAndDelete() throws Exception {
        //show that "application/text" works for delete
        URI fullPathKey1 = rest.fullPathKey("j");
        rest.put(fullPathKey1, "data1", "application/text");
        rest.head(fullPathKey1, HttpStatus.SC_OK);
        rest.delete(fullPathKey1);
        rest.head(fullPathKey1, HttpStatus.SC_NOT_FOUND);

        URI fullPathKey2 = rest.fullPathKey("k");
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        ObjectOutputStream oo = new ObjectOutputStream(bout);
        Integer i1 = 42;
        oo.writeObject(i1);
        oo.flush();
        byte[] byteData = bout.toByteArray();
        rest.put(fullPathKey2, byteData, "application/x-java-serialized-object");
        rest.head(fullPathKey2, HttpStatus.SC_OK);
        rest.delete(fullPathKey2);
        rest.head(fullPathKey2, HttpStatus.SC_NOT_FOUND);
    }

    @Test
    public void testIfModifiedSince() throws Exception {
        URI fullPathKey = rest.fullPathKey(KEY_A);
        rest.put(fullPathKey, "data", "application/text");
        HttpResponse resp = rest.get(fullPathKey);
        String dateLast = resp.getHeaders("Last-Modified")[0].getValue();
        String dateMinus = addDay(dateLast, -1);
        String datePlus = addDay(dateLast, 1);

//        rest.get(fullPathKey, "data", HttpStatus.SC_OK, true,
//                // resource has been modified since
//                "If-Modified-Since", dateMinus);

        rest.get(fullPathKey, null, HttpStatus.SC_NOT_MODIFIED, true,
                // exact same date as stored one
                "If-Modified-Since", dateLast);

//        rest.get(fullPathKey, null, HttpStatus.SC_NOT_MODIFIED, true,
//                // resource hasn't been modified since
//                "If-Modified-Since", datePlus);
    }

    @Test
    public void testIfUnmodifiedSince() throws Exception {
        URI fullPathKey = rest.fullPathKey(KEY_A);
        rest.put(fullPathKey, "data", "application/text");

        HttpResponse resp = rest.get(fullPathKey);
        String dateLast = resp.getHeaders("Last-Modified")[0].getValue();
        String dateMinus = addDay(dateLast, -1);
        String datePlus = addDay(dateLast, 1);

        rest.get(fullPathKey, "data", HttpStatus.SC_OK, true, "If-Unmodified-Since", dateLast);

        rest.get(fullPathKey, "data", HttpStatus.SC_OK, true, "If-Unmodified-Since", datePlus);

        rest.get(fullPathKey, null, HttpStatus.SC_PRECONDITION_FAILED, true, "If-Unmodified-Since", dateMinus);
    }

    @Test
    public void testIfNoneMatch() throws Exception {
        URI fullPathKey = rest.fullPathKey(KEY_A);
        rest.put(fullPathKey, "data", "text/plain");
        HttpResponse resp = rest.get(fullPathKey);
        String eTag = resp.getHeaders("ETag")[0].getValue();

        rest.get(fullPathKey, null, HttpStatus.SC_NOT_MODIFIED, true, "If-None-Match", eTag);

        rest.get(fullPathKey, "data", HttpStatus.SC_OK, true, "If-None-Match", eTag + "garbage");
    }

    @Test
    public void testIfMatch() throws Exception {
        URI fullPathKey = rest.fullPathKey(KEY_A);
        rest.put(fullPathKey, "data", "text/plain");
        HttpResponse resp = rest.get(fullPathKey);

        String eTag = resp.getHeaders("ETag")[0].getValue();
        // test GET with If-Match behaviour
        rest.get(fullPathKey, "data", HttpStatus.SC_OK, true, "If-Match", eTag);
        rest.get(fullPathKey, null, HttpStatus.SC_PRECONDITION_FAILED, true, "If-Match", eTag + "garbage");

        // test HEAD with If-Match behaviour
        rest.head(fullPathKey, HttpStatus.SC_OK, new String[][]{{"If-Match", eTag}});
        rest.head(fullPathKey, HttpStatus.SC_PRECONDITION_FAILED, new String[][]{{"If-Match", eTag + "garbage"}});
    }

    @Test
    public void testNonExistentCache() throws Exception {
        //It seems Common HTTP Client uses different way for parsing HEAD than for other methods.
        //As a consequence it doesn't properly recognise 404 WITH description (without entity it works fine)
        //See https://issues.jboss.org/browse/ISPN-7821
        //rest.head(rest.fullPathKey("nonexistentcache", "nodata"), HttpStatus.SC_NOT_FOUND);
        rest.get(rest.fullPathKey("nonexistentcache", "nodata"), HttpStatus.SC_NOT_FOUND);
        rest.put(rest.fullPathKey("nonexistentcache", "nodata"), "data", "application/text", HttpStatus.SC_NOT_FOUND);
        rest.delete(rest.fullPathKey("nonexistentcache", "nodata"), HttpStatus.SC_NOT_FOUND);
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
        rest.put(rest.fullPathKey(0, KEY_Z), serializedData, "application/x-java-serialized-object");

        HttpResponse resp = rest.get(rest.fullPathKey(0, KEY_Z), null, HttpStatus.SC_OK, false, "Accept",
                "application/x-java-serialized-object");
        ObjectInputStream oin = new ObjectInputStream(resp.getEntity().getContent());
        byte[] dataBack = (byte[]) oin.readObject();
        EntityUtils.consume(resp.getEntity());
        assertTrue(Arrays.equals(data, dataBack));
    }

    @Test
    public void testStoreBigObject() throws Exception {
        int SIZE = 3000000;
        byte[] bytes = new byte[SIZE];
        for (int i = 0; i < SIZE; i++) {
            bytes[i] = (byte) (i % 10);
        }
        rest.put(rest.fullPathKey("object"), bytes, "application/octet-stream");

        HttpResponse resp = rest.getWithoutClose(rest.fullPathKey("object"));
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
        rest.post(rest.fullPathKey(encodedSlashKey), "data", "application/text");
        HttpResponse get = rest.get(rest.fullPathKey(encodedSlashKey), "data");
        assertNotNull(get.getHeaders("ETag")[0].getValue());
        assertNotNull(get.getHeaders("Last-Modified")[0].getValue());
        assertEquals("application/text;charset=UTF-8", get.getHeaders("Content-Type")[0].getValue());
    }
}
