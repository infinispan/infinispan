package org.infinispan.it.endpoints;

import static java.util.Collections.singletonMap;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_JSON_TYPE;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_SERIALIZED_OBJECT;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_SERIALIZED_OBJECT_TYPE;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_XML_TYPE;
import static org.infinispan.commons.dataconversion.MediaType.TEXT_PLAIN;
import static org.infinispan.commons.dataconversion.MediaType.TEXT_PLAIN_TYPE;
import static org.infinispan.rest.JSONConstants.TYPE;
import static org.infinispan.rest.ResponseHeader.CONTENT_TYPE_HEADER;
import static org.infinispan.util.concurrent.CompletionStages.join;
import static org.testng.AssertJUnit.assertArrayEquals;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import org.infinispan.client.hotrod.Flag;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.rest.RestCacheClient;
import org.infinispan.client.rest.RestEntity;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.rest.ResponseHeader;
import org.infinispan.test.AbstractInfinispanTest;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Test embedded caches, Hot Rod, and REST endpoints.
 *
 * @author Galder Zamarreño
 * @since 5.3
 */
@Test(groups = "functional", testName = "it.endpoints.EmbeddedRestHotRodTest")
public class EmbeddedRestHotRodTest extends AbstractInfinispanTest {

   private static final DateFormat dateFormat = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss zzz", Locale.US);

   EndpointsCacheFactory<String, Object> cacheFactory;

   @BeforeClass
   protected void setup() throws Exception {
      cacheFactory = new EndpointsCacheFactory.Builder<String, Object>().withCacheMode(CacheMode.LOCAL)
            .withContextInitializer(EndpointITSCI.INSTANCE).build();
      dateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
      cacheFactory.addRegexAllowList("org.infinispan.*Person");
   }

   @AfterClass
   protected void teardown() {
      EndpointsCacheFactory.killCacheFactories(cacheFactory);
   }

   public void testRestPutEmbeddedHotRodGet() {
      final String key = "1";

      // 1. Put with REST
      RestCacheClient restCacheClient = cacheFactory.getRestCacheClient();
      CompletionStage<RestResponse> response = restCacheClient.put(key, RestEntity.create(TEXT_PLAIN, "<hey>ho</hey>"));
      assertEquals(204, join(response).getStatus());

      // 2. Get with Embedded
      assertEquals("<hey>ho</hey>", cacheFactory.getEmbeddedCache().get(key));

      // 3. Get with Hot Rod
      assertEquals("<hey>ho</hey>", cacheFactory.getHotRodCache().get(key));
   }

   public void testEmbeddedPutRestHotRodGet() {
      final String key = "2";

      // 1. Put with Embedded
      assertNull(cacheFactory.getEmbeddedCache().put(key, "v1"));

      // 2. Get with Hot Rod
      assertEquals("v1", cacheFactory.getHotRodCache().get(key));

      // 3. Get with REST
      RestResponse response = join(cacheFactory.getRestCacheClient().get(key, TEXT_PLAIN_TYPE));
      assertEquals(200, response.getStatus());
      assertEquals("v1", response.getBody());
   }

   public void testHotRodPutEmbeddedRestGet() {
      final String key = "3";

      // 1. Put with Hot Rod
      RemoteCache<String, Object> remote = cacheFactory.getHotRodCache();
      assertNull(remote.withFlags(Flag.FORCE_RETURN_VALUE).put(key, "v1"));

      // 2. Get with Embedded
      assertEquals("v1", cacheFactory.getEmbeddedCache().get(key));

      // 3. Get with REST
      RestResponse response = join(cacheFactory.getRestCacheClient().get(key, TEXT_PLAIN_TYPE));
      assertEquals(200, response.getStatus());
      assertEquals("v1", response.getBody());
   }

   public void testCustomObjectHotRodPutEmbeddedRestGet() throws Exception {
      final String key = "4";
      Person p = new Person("Martin");

      // 1. Put with Hot Rod
      RemoteCache<String, Object> remote = cacheFactory.getHotRodCache();
      assertNull(remote.withFlags(Flag.FORCE_RETURN_VALUE).put(key, p));

      // 2. Get with Embedded
      assertEquals(p, cacheFactory.getEmbeddedCache().get(key));

      // 3. Get with REST
      RestResponse response = join(cacheFactory.getRestCacheClient().get(key, APPLICATION_SERIALIZED_OBJECT_TYPE));
      assertEquals(200, response.getStatus());

      // REST finds the Java POJO in-memory and returns the Java serialized version
      assertEquals(p, new ObjectInputStream(response.getBodyAsStream()).readObject());
   }

   public void testCustomObjectEmbeddedPutHotRodRestGet() throws Exception {
      final String key = "5";
      Person p = new Person("Galder");

      // 1. Put with Embedded
      assertNull(cacheFactory.getEmbeddedCache().put(key, p));

      // 2. Get with Hot Rod
      assertEquals(p, cacheFactory.getHotRodCache().get(key));

      // 3. Get with REST
      RestResponse response = join(cacheFactory.getRestCacheClient()
            .get(key, "application/x-java-serialized-object, application/json;q=0.3"));
      assertEquals(200, response.getStatus());

      // REST finds the Java POJO in-memory and returns the Java serialized version
      assertEquals(p, new ObjectInputStream(response.getBodyAsStream()).readObject());
   }

   public void testCustomObjectEmbeddedPutRestGetAcceptJSONAndXML() {
      final String key = "6";
      final Person p = new Person("Anna");

      // 1. Put with Embedded
      assertNull(cacheFactory.getEmbeddedCache().put(key, p));

      // 2. Get with REST (accept application/json)
      RestResponse response = join(cacheFactory.getRestCacheClient().get(key, APPLICATION_JSON_TYPE));
      String body = response.getBody();
      assertEquals(200, response.getStatus());
      assertEquals(asJson(p), body);

      // 3. Get with REST (accept application/xml)
      response = join(cacheFactory.getRestCacheClient().get(key, APPLICATION_XML_TYPE));
      assertEquals(200, response.getStatus());
      assertTrue(response.getBody().contains("<name>Anna</name>"));
   }

   public void testCustomObjectHotRodPutRestGetAcceptJSONAndXML() {
      final String key = "7";
      final Person p = new Person("Jakub");

      // 1. Put with HotRod
      RemoteCache<String, Object> remote = cacheFactory.getHotRodCache();
      assertNull(remote.withFlags(Flag.FORCE_RETURN_VALUE).put(key, p));

      // 2. Get with REST (accept application/json)
      RestResponse response = join(cacheFactory.getRestCacheClient().get(key, APPLICATION_JSON_TYPE));
      assertEquals(200, response.getStatus());
      assertEquals(asJson(p), response.getBody());

      // 3. Get with REST (accept application/xml)
      response = join(cacheFactory.getRestCacheClient().get(key, APPLICATION_XML_TYPE));
      assertEquals(200, response.getStatus());
      assertTrue(response.getBody().contains("<name>Jakub</name>"));
   }

   public void testCustomObjectRestPutHotRodEmbeddedGet() throws Exception {
      final String key = "77";
      Person p = new Person("Iker");

      // 1. Put with Rest
      RestCacheClient restClient = cacheFactory.getRestCacheClient();

      ByteArrayOutputStream bout = new ByteArrayOutputStream();
      try (ObjectOutputStream oos = new ObjectOutputStream(bout)) {
         oos.writeObject(p);
      }
      RestEntity value = RestEntity.create(APPLICATION_SERIALIZED_OBJECT, new ByteArrayInputStream(bout.toByteArray()));
      join(restClient.put(key, value));

      // 2. Get with Hot Rod
      RemoteCache<String, Object> remote = cacheFactory.getHotRodCache();
      assertEquals(p, remote.get(key));

      // 3. Get with Embedded
      assertEquals(p, cacheFactory.getEmbeddedCache().getAdvancedCache().get(key));
   }

   public void testHotRodEmbeddedPutRestHeadExpiry() throws Exception {
      final String key1 = "8";
      final String key2 = "9";

      // 1. Put with HotRod
      assertNull(cacheFactory.getHotRodCache().put(key1, "v1", 5, TimeUnit.SECONDS));

      // 2. Put with Embedded
      assertNull(cacheFactory.getEmbeddedCache().put(key2, "v2", 5, TimeUnit.SECONDS));

      // 3. HEAD with REST key1
      RestResponse response = join(cacheFactory.getRestCacheClient().head(key1));
      assertEquals(200, response.getStatus());
      String expires = response.getHeader(ResponseHeader.EXPIRES_HEADER.getValue());
      assertNotNull(expires);
      assertTrue(dateFormat.parse(expires).after(new GregorianCalendar(2013, Calendar.JANUARY, 1).getTime()));

      // 4. HEAD with REST key2
      response = join(cacheFactory.getRestCacheClient().head(key2));
      assertEquals(200, response.getStatus());
      expires = response.getHeader(ResponseHeader.EXPIRES_HEADER.getValue());
      assertNotNull(expires);
   }

   public void testHotRodEmbeddedPutRestGetExpiry() throws Exception {
      final String key = "10";
      final String key2 = "11";

      // 1. Put with HotRod
      assertNull(cacheFactory.getHotRodCache().put(key, "v1", 5, TimeUnit.SECONDS));

      // 2. Put with Embedded
      assertNull(cacheFactory.getEmbeddedCache().put(key2, "v2", 5, TimeUnit.SECONDS));

      // 3. Get with REST key
      RestResponse response = join(cacheFactory.getRestCacheClient().get(key));
      assertEquals(200, response.getStatus());
      assertDate(response, "Expires");

      // 4. Get with REST key2
      response = join(cacheFactory.getRestCacheClient().get(key2));
      assertEquals(200, response.getStatus());
      assertDate(response, "Expires");
   }

   public void testHotRodEmbeddedPutRestGetLastModified() throws Exception {
      final String key = "12";
      final String key2 = "13";

      // 1. Put with HotRod
      assertNull(cacheFactory.getHotRodCache().put(key, "v1", 5, TimeUnit.SECONDS));

      // 2. Put with Embedded
      assertNull(cacheFactory.getEmbeddedCache().put(key2, "v2", 5, TimeUnit.SECONDS));

      // 3. Get with REST key
      RestResponse response = join(cacheFactory.getRestCacheClient().get(key));
      assertEquals(200, response.getStatus());
      assertDate(response, "Last-Modified");

      // 4. Get with REST key2
      response = join(cacheFactory.getRestCacheClient().get(key2));
      assertEquals(200, response.getStatus());
      assertDate(response, "Last-Modified");
   }

   private static void assertDate(RestResponse response, String header) throws Exception {
      String dateHeader = response.getHeader(header);
      assertNotNull(dateHeader);
      Date parsedDate = dateFormat.parse(dateHeader);
      assertTrue("Parsed date is before this code was written: " + parsedDate,
            parsedDate.after(new GregorianCalendar(2013, Calendar.JANUARY, 1).getTime()));
   }

   public void testByteArrayHotRodEmbeddedPutRestGet() {
      final String key1 = "14";
      final String key2 = "15";

      // 1. Put with Hot Rod
      RemoteCache<String, Object> remote = cacheFactory.getHotRodCache();
      assertNull(remote.withFlags(Flag.FORCE_RETURN_VALUE).put(key1, "v1".getBytes()));

      // 2. Put with Embedded
      assertNull(cacheFactory.getEmbeddedCache().put(key2, "v2".getBytes()));

      // 3. Get with REST key1
      RestResponse response = join(cacheFactory.getRestCacheClient().get(key1, TEXT_PLAIN_TYPE));
      assertEquals(200, response.getStatus());
      assertEquals(TEXT_PLAIN_TYPE, response.getHeader(CONTENT_TYPE_HEADER.getValue()));
      assertArrayEquals("v1".getBytes(), response.getBodyAsByteArray());

      // 4. Get with REST key2
      response = join(cacheFactory.getRestCacheClient().get(key2, TEXT_PLAIN_TYPE));
      assertEquals(200, response.getStatus());
      assertEquals(TEXT_PLAIN_TYPE, response.getHeader(CONTENT_TYPE_HEADER.getValue()));
      assertArrayEquals("v2".getBytes(), response.getBodyAsByteArray());
   }

   public void testHotRodEmbeddedPutRestGetWrongAccept() {
      final String key1 = "16";
      final String key2 = "17";

      // 1. Put with HotRod
      assertNull(cacheFactory.getHotRodCache().put(key1, "v1"));

      // 2. Put with Embedded
      assertNull(cacheFactory.getEmbeddedCache().put(key2, "v2"));

      // 3. GET with REST key1
      RestResponse response = join(cacheFactory.getRestCacheClient().get(key1, "unknown-media-type"));
      assertEquals(406, response.getStatus());

      // 4. GET with REST key2
      response = join(cacheFactory.getRestCacheClient().get(key2, "unknown-media-type"));
      assertEquals(406, response.getStatus());
   }

   public void testHotRodEmbeddedPutRestGetCacheControlHeader() {
      final String key1 = "18";
      final String key2 = "19";

      // 1. Put with HotRod
      assertNull(cacheFactory.getHotRodCache().put(key1, "v1", 7, TimeUnit.SECONDS));

      // 2. Put with Embedded
      assertNull(cacheFactory.getEmbeddedCache().put(key2, "v2", 7, TimeUnit.SECONDS));

      // 3. GET with REST key1, long min-fresh
      Map<String, String> headers = singletonMap("Cache-Control", "min-fresh=20");
      RestResponse response = join(cacheFactory.getRestCacheClient().get(key1, headers));
      assertEquals(404, response.getStatus());

      // 4. GET with REST key2, long min-fresh
      response = join(cacheFactory.getRestCacheClient().get(key2, headers));
      assertEquals(404, response.getStatus());

      // 5. GET with REST key1, short min-fresh
      headers = new HashMap<>();
      headers.put("Accept", TEXT_PLAIN_TYPE);
      headers.put("Cache-Control", "min-fresh=3");
      response = join(cacheFactory.getRestCacheClient().get(key1, headers));

      assertEquals(200, response.getStatus());
      assertNotNull(response.getHeader("Cache-Control"));
      assertTrue(response.getHeader("Cache-Control").contains("max-age"));
      assertEquals("v1", response.getBody());

      // 6. GET with REST key2, short min-fresh
      response = join(cacheFactory.getRestCacheClient().get(key2, headers));
      assertEquals(200, response.getStatus());
      assertTrue(response.getHeader("Cache-Control").contains("max-age"));
      assertEquals("v2", response.getBody());
   }

   private String asJson(Person p) {
      Json person = Json.object();
      person.set(TYPE, p.getClass().getName());
      person.set("name", p.name);
      return person.toString();
   }

   /**
    * The class needs a getter for the attribute "name" so that it can be converted to JSON format
    * internally by the REST server.
    */
   static class Person implements Serializable {

      @ProtoField(number = 1)
      final String name;

      @ProtoFactory
      Person(String name) {
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

         return name.equals(person.name);
      }

      @Override
      public int hashCode() {
         return name.hashCode();
      }
   }
}
