package org.infinispan.it.compatibility;

import org.apache.commons.httpclient.Header;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.methods.ByteArrayRequestEntity;
import org.apache.commons.httpclient.methods.EntityEnclosingMethod;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.HeadMethod;
import org.apache.commons.httpclient.methods.PutMethod;
import org.infinispan.client.hotrod.Flag;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.configuration.cache.CacheMode;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import javax.servlet.http.HttpServletResponse;

import java.io.ObjectInputStream;
import java.io.Serializable;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Locale;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import static org.testng.AssertJUnit.*;
import static org.testng.AssertJUnit.assertEquals;

/**
 * Test compatibility between embedded caches, Hot Rod, and REST endpoints.
 *
 * @author Galder Zamarreño
 * @since 5.3
 */
@Test(groups = "functional", testName = "it.compatibility.EmbeddedRestHotRodTest")
public class EmbeddedRestHotRodTest {

   private static final DateFormat dateFormat = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss zzz", Locale.US);

   CompatibilityCacheFactory<String, Object> cacheFactory;

   @BeforeClass
   protected void setup() throws Exception {
      cacheFactory = new CompatibilityCacheFactory<String, Object>(CacheMode.LOCAL).setup();
      dateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
   }

   @AfterClass
   protected void teardown() {
      CompatibilityCacheFactory.killCacheFactories(cacheFactory);
   }

   public void testRestPutEmbeddedHotRodGet() throws Exception {
      final String key = "1";

      // 1. Put with REST
      EntityEnclosingMethod put = new PutMethod(cacheFactory.getRestUrl() + "/" + key);
      put.setRequestEntity(new ByteArrayRequestEntity(
            "<hey>ho</hey>".getBytes(), "application/octet-stream"));
      HttpClient restClient = cacheFactory.getRestClient();
      restClient.executeMethod(put);
      assertEquals(HttpServletResponse.SC_OK, put.getStatusCode());
      assertEquals("", put.getResponseBodyAsString().trim());

      // 2. Get with Embedded
      assertArrayEquals("<hey>ho</hey>".getBytes(), (byte[])
            cacheFactory.getEmbeddedCache().get(key));

      // 3. Get with Hot Rod
      assertArrayEquals("<hey>ho</hey>".getBytes(), (byte[])
            cacheFactory.getHotRodCache().get(key));
   }

   public void testEmbeddedPutRestHotRodGet() throws Exception {
      final String key = "2";

      // 1. Put with Embedded
      assertEquals(null, cacheFactory.getEmbeddedCache().put(key, "v1"));

      // 2. Get with Hot Rod
      assertEquals("v1", cacheFactory.getHotRodCache().get(key));

      // 3. Get with REST
      HttpMethod get = new GetMethod(cacheFactory.getRestUrl() + "/" + key);
      cacheFactory.getRestClient().executeMethod(get);
      assertEquals(HttpServletResponse.SC_OK, get.getStatusCode());
      assertEquals("v1", get.getResponseBodyAsString());
   }

   public void testHotRodPutEmbeddedRestGet() throws Exception {
      final String key = "3";

      // 1. Put with Hot Rod
      RemoteCache<String, Object> remote = cacheFactory.getHotRodCache();
      assertEquals(null, remote.withFlags(Flag.FORCE_RETURN_VALUE).put(key, "v1"));

      // 2. Get with Embedded
      assertEquals("v1", cacheFactory.getEmbeddedCache().get(key));

      // 3. Get with REST
      HttpMethod get = new GetMethod(cacheFactory.getRestUrl() + "/" + key);
      cacheFactory.getRestClient().executeMethod(get);
      assertEquals(HttpServletResponse.SC_OK, get.getStatusCode());
      assertEquals("v1", get.getResponseBodyAsString());
   }

   public void testCustomObjectHotRodPutEmbeddedRestGet() throws Exception{
      final String key = "4";
      Person p = new Person("Martin");

      // 1. Put with Hot Rod
      RemoteCache<String, Object> remote = cacheFactory.getHotRodCache();
      assertEquals(null, remote.withFlags(Flag.FORCE_RETURN_VALUE).put(key, p));

      // 2. Get with Embedded
      assertEquals(p, cacheFactory.getEmbeddedCache().get(key));

      // 3. Get with REST
      HttpMethod get = new GetMethod(cacheFactory.getRestUrl() + "/" + key);
      get.setRequestHeader("Accept", "application/x-java-serialized-object");
      cacheFactory.getRestClient().executeMethod(get);
      assertEquals(get.getStatusText(), HttpServletResponse.SC_OK, get.getStatusCode());
      // REST finds the Java POJO in-memory and returns the Java serialized version
      assertEquals(p, new ObjectInputStream(get.getResponseBodyAsStream()).readObject());
   }

   public void testCustomObjectEmbeddedPutHotRodRestGet() throws Exception{
      final String key = "5";
      Person p = new Person("Galder");

      // 1. Put with Embedded
      assertEquals(null, cacheFactory.getEmbeddedCache().put(key, p));

      // 2. Get with Hot Rod
      assertEquals(p, cacheFactory.getHotRodCache().get(key));

      // 3. Get with REST
      HttpMethod get = new GetMethod(cacheFactory.getRestUrl() + "/" + key);
      get.setRequestHeader("Accept", "application/x-java-serialized-object, application/json;q=0.3");

      cacheFactory.getRestClient().executeMethod(get);
      assertEquals("application/x-java-serialized-object", get.getResponseHeader("Content-Type").getValue());
      assertEquals(get.getStatusText(), HttpServletResponse.SC_OK, get.getStatusCode());
      // REST finds the Java POJO in-memory and returns the Java serialized version
      assertEquals(p, new ObjectInputStream(get.getResponseBodyAsStream()).readObject());
   }

   public void testCustomObjectEmbeddedPutRestGetAcceptJSONAndXML() throws Exception{
      final String key = "6";
      final Person p = new Person("Anna");

      // 1. Put with Embedded
      assertEquals(null, cacheFactory.getEmbeddedCache().put(key, p));

      // 2. Get with REST (accept application/json)
      HttpMethod getJson = new GetMethod(cacheFactory.getRestUrl() + "/" + key);
      getJson.setRequestHeader("Accept", "application/json");
      cacheFactory.getRestClient().executeMethod(getJson);
      assertEquals(getJson.getStatusText(), HttpServletResponse.SC_OK, getJson.getStatusCode());
      assertEquals("{\"name\":\"Anna\"}", getJson.getResponseBodyAsString());

      // 3. Get with REST (accept application/xml)
      HttpMethod getXml = new GetMethod(cacheFactory.getRestUrl() + "/" + key);
      getXml.setRequestHeader("Accept", "application/xml");
      cacheFactory.getRestClient().executeMethod(getXml);
      assertEquals(getXml.getStatusText(), HttpServletResponse.SC_OK, getXml.getStatusCode());
      assertTrue(getXml.getResponseBodyAsString().contains("<name>Anna</name>"));
   }

   public void testCustomObjectHotRodPutRestGetAcceptJSONAndXML() throws Exception{
      final String key = "7";
      final Person p = new Person("Jakub");

      // 1. Put with HotRod
      RemoteCache<String, Object> remote = cacheFactory.getHotRodCache();
      assertEquals(null, remote.withFlags(Flag.FORCE_RETURN_VALUE).put(key, p));

      // 2. Get with REST (accept application/json)
      HttpMethod getJson = new GetMethod(cacheFactory.getRestUrl() + "/" + key);
      getJson.setRequestHeader("Accept", "application/json");
      cacheFactory.getRestClient().executeMethod(getJson);
      assertEquals(getJson.getStatusText(), HttpServletResponse.SC_OK, getJson.getStatusCode());
      assertEquals("{\"name\":\"Jakub\"}", getJson.getResponseBodyAsString());

      // 3. Get with REST (accept application/xml)
      HttpMethod getXml = new GetMethod(cacheFactory.getRestUrl() + "/" + key);
      getXml.setRequestHeader("Accept", "application/xml");
      cacheFactory.getRestClient().executeMethod(getXml);
      assertEquals(getXml.getStatusText(), HttpServletResponse.SC_OK, getXml.getStatusCode());
      assertTrue(getXml.getResponseBodyAsString().contains("<name>Jakub</name>"));
   }

   public void testHotRodEmbeddedPutRestHeadExpiry() throws Exception {
      final String key1 = "8";
      final String key2 = "9";

      // 1. Put with HotRod
      assertEquals(null, cacheFactory.getHotRodCache().put(key1, "v1", 5, TimeUnit.SECONDS));

      // 2. Put with Embedded
      assertEquals(null, cacheFactory.getEmbeddedCache().put(key2, "v2", 5, TimeUnit.SECONDS));

      // 3. HEAD with REST key1
      HttpMethod headKey1 = new HeadMethod(cacheFactory.getRestUrl() + "/" + key1);
      cacheFactory.getRestClient().executeMethod(headKey1);
      assertEquals(HttpServletResponse.SC_OK, headKey1.getStatusCode());
      Header expires = headKey1.getResponseHeader("Expires");
      assertNotNull(expires);
      assertTrue(dateFormat.parse(expires.getValue()).after(new GregorianCalendar(2013, 1, 1).getTime()));

      // 4. HEAD with REST key2
      HttpMethod headKey2 = new HeadMethod(cacheFactory.getRestUrl() + "/" + key2);
      cacheFactory.getRestClient().executeMethod(headKey2);
      assertEquals(HttpServletResponse.SC_OK, headKey2.getStatusCode());
      assertNotNull(headKey2.getResponseHeader("Expires"));
   }

   public void testHotRodEmbeddedPutRestGetExpiry() throws Exception {
      final String key = "10";
      final String key2 = "11";

      // 1. Put with HotRod
      assertEquals(null, cacheFactory.getHotRodCache().put(key, "v1", 5, TimeUnit.SECONDS));

      // 2. Put with Embedded
      assertEquals(null, cacheFactory.getEmbeddedCache().put(key2, "v2", 5, TimeUnit.SECONDS));

      // 3. Get with REST key
      HttpMethod get1 = new GetMethod(cacheFactory.getRestUrl() + "/" + key);
      cacheFactory.getRestClient().executeMethod(get1);
      assertEquals(HttpServletResponse.SC_OK, get1.getStatusCode());
      assertDate(get1, "Expires");

      // 4. Get with REST key2
      HttpMethod get2 = new GetMethod(cacheFactory.getRestUrl() + "/" + key2);
      cacheFactory.getRestClient().executeMethod(get2);
      assertEquals(HttpServletResponse.SC_OK, get2.getStatusCode());
      assertDate(get2, "Expires");
   }

   public void testHotRodEmbeddedPutRestGetLastModified() throws Exception {
      final String key = "12";
      final String key2 = "13";

      // 1. Put with HotRod
      assertEquals(null, cacheFactory.getHotRodCache().put(key, "v1", 5, TimeUnit.SECONDS));

      // 2. Put with Embedded
      assertEquals(null, cacheFactory.getEmbeddedCache().put(key2, "v2", 5, TimeUnit.SECONDS));

      // 3. Get with REST key
      HttpMethod get1 = new GetMethod(cacheFactory.getRestUrl() + "/" + key);
      cacheFactory.getRestClient().executeMethod(get1);
      assertEquals(HttpServletResponse.SC_OK, get1.getStatusCode());
      assertDate(get1, "Last-Modified");

      // 4. Get with REST key2
      HttpMethod get2 = new GetMethod(cacheFactory.getRestUrl() + "/" + key2);
      cacheFactory.getRestClient().executeMethod(get2);
      assertEquals(HttpServletResponse.SC_OK, get2.getStatusCode());
      assertDate(get2, "Last-Modified");
   }

   private static void assertDate(HttpMethod method, String header) throws Exception {
      Header dateHeader = method.getResponseHeader(header);
      assertNotNull(dateHeader);
      Date parsedDate = dateFormat.parse(dateHeader.getValue());
      assertTrue("Parsed date is before this code was written: " + parsedDate,
            parsedDate.after(new GregorianCalendar(2013, 1, 1).getTime()));
   }

   public void testByteArrayHotRodEmbeddedPutRestGet() throws Exception{
      final String key1 = "14";
      final String key2 = "15";

      // 1. Put with Hot Rod
      RemoteCache<String, Object> remote = cacheFactory.getHotRodCache();
      assertEquals(null, remote.withFlags(Flag.FORCE_RETURN_VALUE).put(key1, "v1".getBytes()));

      // 2. Put with Embedded
      assertEquals(null, cacheFactory.getEmbeddedCache().put(key2, "v2".getBytes()));

      // 3. Get with REST key1
      HttpMethod getHotRodValue = new GetMethod(cacheFactory.getRestUrl() + "/" + key1);
      cacheFactory.getRestClient().executeMethod(getHotRodValue);
      assertEquals(getHotRodValue.getStatusText(), HttpServletResponse.SC_OK, getHotRodValue.getStatusCode());
      assertEquals("application/octet-stream", getHotRodValue.getResponseHeader("Content-Type").getValue());
      assertArrayEquals("v1".getBytes(), getHotRodValue.getResponseBody());

      // 4. Get with REST key2
      HttpMethod getEmbeddedValue = new GetMethod(cacheFactory.getRestUrl() + "/" + key2);
      cacheFactory.getRestClient().executeMethod(getEmbeddedValue);
      assertEquals(getEmbeddedValue.getStatusText(), HttpServletResponse.SC_OK, getEmbeddedValue.getStatusCode());
      assertEquals("application/octet-stream", getEmbeddedValue.getResponseHeader("Content-Type").getValue());
      assertArrayEquals("v2".getBytes(), getEmbeddedValue.getResponseBody());
   }

   public void testHotRodEmbeddedPutRestGetWrongAccept() throws Exception {
      final String key1 = "16";
      final String key2 = "17";

      // 1. Put with HotRod
      assertEquals(null, cacheFactory.getHotRodCache().put(key1, "v1"));

      // 2. Put with Embedded
      assertEquals(null, cacheFactory.getEmbeddedCache().put(key2, "v2"));

      // 3. GET with REST key1
      HttpMethod getKey1 = new HeadMethod(cacheFactory.getRestUrl() + "/" + key1);
      getKey1.setRequestHeader("Accept", "unknown-media-type");
      cacheFactory.getRestClient().executeMethod(getKey1);
      assertEquals(getKey1.getStatusText(), HttpServletResponse.SC_BAD_REQUEST, getKey1.getStatusCode());

      // 4. GET with REST key2
      HttpMethod getKey2 = new HeadMethod(cacheFactory.getRestUrl() + "/" + key2);
      getKey2.setRequestHeader("Accept", "unknown-media-type");
      cacheFactory.getRestClient().executeMethod(getKey2);
      assertEquals(getKey2.getStatusText(), HttpServletResponse.SC_BAD_REQUEST, getKey2.getStatusCode());
   }

   public void testHotRodEmbeddedPutRestGetCacheControlHeader() throws Exception {
      final String key1 = "18";
      final String key2 = "19";

      // 1. Put with HotRod
      assertEquals(null, cacheFactory.getHotRodCache().put(key1, "v1", 7, TimeUnit.SECONDS));

      // 2. Put with Embedded
      assertEquals(null, cacheFactory.getEmbeddedCache().put(key2, "v2", 7, TimeUnit.SECONDS));

      // 3. GET with REST key1, long min-fresh
      HttpMethod getKey1 = new GetMethod(cacheFactory.getRestUrl() + "/" + key1);
      getKey1.setRequestHeader("Cache-Control", "min-fresh=20");
      cacheFactory.getRestClient().executeMethod(getKey1);
      assertEquals(getKey1.getStatusText(), HttpServletResponse.SC_NOT_FOUND, getKey1.getStatusCode());

      // 4. GET with REST key2, long min-fresh
      HttpMethod getKey2 = new GetMethod(cacheFactory.getRestUrl() + "/" + key2);
      getKey2.setRequestHeader("Cache-Control", "min-fresh=20");
      cacheFactory.getRestClient().executeMethod(getKey2);
      assertEquals(getKey2.getStatusText(), HttpServletResponse.SC_NOT_FOUND, getKey2.getStatusCode());

      // 5. GET with REST key1, short min-fresh
      getKey1 = new GetMethod(cacheFactory.getRestUrl() + "/" + key1);
      getKey1.setRequestHeader("Cache-Control", "min-fresh=3");
      cacheFactory.getRestClient().executeMethod(getKey1);
      assertNotNull(getKey1.getResponseHeader("Cache-Control"));
      assertTrue(getKey1.getResponseHeader("Cache-Control").getValue().contains("max-age"));
      assertEquals(getKey1.getStatusText(), HttpServletResponse.SC_OK, getKey1.getStatusCode());
      assertEquals("v1", getKey1.getResponseBodyAsString());

      // 6. GET with REST key2, short min-fresh
      getKey2 = new GetMethod(cacheFactory.getRestUrl() + "/" + key2);
      getKey2.setRequestHeader("Cache-Control", "min-fresh=3");
      cacheFactory.getRestClient().executeMethod(getKey2);
      assertTrue(getKey2.getResponseHeader("Cache-Control").getValue().contains("max-age"));
      assertEquals(getKey2.getStatusText(), HttpServletResponse.SC_OK, getKey2.getStatusCode());
      assertEquals("v2", getKey2.getResponseBodyAsString());
   }

   /**
    * The class needs a getter for the attribute "name" so that it can be converted to JSON format
    * internally by the REST server.
    */
   static class Person implements Serializable {

      final String name;

      public Person(String name) {
         this.name = name;
      }

      public String getName() {
         return name;
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (o == null || getClass() != o.getClass()) return false;

         Person person = (Person) o;

         if (!name.equals(person.name)) return false;

         return true;
      }

      @Override
      public int hashCode() {
         return name.hashCode();
      }
   }

}
