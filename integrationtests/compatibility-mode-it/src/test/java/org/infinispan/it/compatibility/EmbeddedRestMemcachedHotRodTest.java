package org.infinispan.it.compatibility;

import net.spy.memcached.CASValue;
import net.spy.memcached.CachedData;
import net.spy.memcached.transcoders.SerializingTranscoder;
import net.spy.memcached.transcoders.Transcoder;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.methods.ByteArrayRequestEntity;
import org.apache.commons.httpclient.methods.EntityEnclosingMethod;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PutMethod;
import org.infinispan.client.hotrod.Flag;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.VersionedValue;
import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.commons.io.ByteBufferImpl;
import org.infinispan.commons.marshall.AbstractMarshaller;
import org.infinispan.configuration.cache.CacheMode;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import javax.servlet.http.HttpServletResponse;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.testng.AssertJUnit.*;

/**
 * Test compatibility between embedded caches, Hot Rod, REST and Memcached endpoints.
 *
 * @author Galder Zamarreño
 * @since 5.3
 */
@Test(groups = {"functional", "smoke"}, testName = "it.compatibility.EmbeddedRestMemcachedHotRodTest")
public class EmbeddedRestMemcachedHotRodTest {

   final static String CACHE_NAME = "memcachedCache";

   CompatibilityCacheFactory<String, Object> cacheFactory;

   @BeforeClass
   protected void setup() throws Exception {
      cacheFactory = new CompatibilityCacheFactory<String, Object>(
            CACHE_NAME, new SpyMemcachedCompatibleMarshaller(), CacheMode.LOCAL).setup();
   }

   @AfterClass
   protected void teardown() {
      CompatibilityCacheFactory.killCacheFactories(cacheFactory);
   }

   public void testMemcachedPutEmbeddedRestHotRodGetTest() throws Exception {
      final String key = "1";

      // 1. Put with Memcached
      Future<Boolean> f = cacheFactory.getMemcachedClient().set(key, 0, "v1");
      assertTrue(f.get(60, TimeUnit.SECONDS));

      // 2. Get with Embedded
      assertEquals("v1", cacheFactory.getEmbeddedCache().get(key));

      // 3. Get with REST
      HttpMethod get = new GetMethod(cacheFactory.getRestUrl() + "/" + key);
      cacheFactory.getRestClient().executeMethod(get);
      assertEquals(HttpServletResponse.SC_OK, get.getStatusCode());
      assertEquals("text/plain", get.getResponseHeader("Content-Type").getValue());
      assertEquals("v1", get.getResponseBodyAsString());

      // 4. Get with Hot Rod
      assertEquals("v1", cacheFactory.getHotRodCache().get(key));
   }

   public void testEmbeddedPutMemcachedRestHotRodGetTest() throws Exception {
      final String key = "2";

      // 1. Put with Embedded
      assertEquals(null, cacheFactory.getEmbeddedCache().put(key, "v1"));

      // 2. Get with Memcached
      assertEquals("v1", cacheFactory.getMemcachedClient().get(key));

      // 3. Get with REST
      HttpMethod get = new GetMethod(cacheFactory.getRestUrl() + "/" + key);
      cacheFactory.getRestClient().executeMethod(get);
      assertEquals(HttpServletResponse.SC_OK, get.getStatusCode());
      assertEquals("v1", get.getResponseBodyAsString());

      // 4. Get with Hot Rod
      assertEquals("v1", cacheFactory.getHotRodCache().get(key));
   }

   public void testRestPutEmbeddedMemcachedHotRodGetTest() throws Exception {
      final String key = "3";

      // 1. Put with REST
      EntityEnclosingMethod put = new PutMethod(cacheFactory.getRestUrl() + "/" + key);
      put.setRequestEntity(new ByteArrayRequestEntity(
            "<hey>ho</hey>".getBytes(), "application/octet-stream"));
      HttpClient restClient = cacheFactory.getRestClient();
      restClient.executeMethod(put);
      assertEquals(HttpServletResponse.SC_OK, put.getStatusCode());
      assertEquals("", put.getResponseBodyAsString().trim());

      // 2. Get with Embedded (given a marshaller, it can unmarshall the result)
      assertEquals("<hey>ho</hey>",
            cacheFactory.getEmbeddedCache().get(key));

      // 3. Get with Memcached (given a marshaller, it can unmarshall the result)
      assertEquals("<hey>ho</hey>",
            cacheFactory.getMemcachedClient().get(key));

      // 4. Get with Hot Rod (given a marshaller, it can unmarshall the result)
      assertEquals("<hey>ho</hey>",
            cacheFactory.getHotRodCache().get(key));
   }

   public void testHotRodPutEmbeddedMemcachedRestGetTest() throws Exception {
      final String key = "4";

      // 1. Put with Hot Rod
      RemoteCache<String, Object> remote = cacheFactory.getHotRodCache();
      assertEquals(null, remote.withFlags(Flag.FORCE_RETURN_VALUE).put(key, "v1"));

      // 2. Get with Embedded
      assertEquals("v1", cacheFactory.getEmbeddedCache().get(key));

      // 3. Get with Memcached
      assertEquals("v1", cacheFactory.getMemcachedClient().get(key));

      // 4. Get with REST
      HttpMethod get = new GetMethod(cacheFactory.getRestUrl() + "/" + key);
      cacheFactory.getRestClient().executeMethod(get);
      assertEquals(HttpServletResponse.SC_OK, get.getStatusCode());
      assertEquals("v1", get.getResponseBodyAsString());
   }

   public void testEmbeddedReplaceMemcachedCASTest() throws Exception {
      final String key1 = "5";

      // 1. Put with Memcached
      Future<Boolean> f = cacheFactory.getMemcachedClient().set(key1, 0, "v1");
      assertTrue(f.get(60, TimeUnit.SECONDS));
      CASValue oldValue = cacheFactory.getMemcachedClient().gets(key1);

      // 2. Replace with Embedded
      assertTrue(cacheFactory.getEmbeddedCache().replace(key1, "v1", "v2"));

      // 4. Get with Memcached and verify value/CAS
      CASValue newValue = cacheFactory.getMemcachedClient().gets(key1);
      assertEquals("v2", newValue.getValue());
      assertNotSame("The version (CAS) should have changed, " +
            "oldCase=" + oldValue.getCas() + ", newCas=" + newValue.getCas(),
            oldValue.getCas(), newValue.getCas());
   }

   public void testHotRodReplaceMemcachedCASTest() throws Exception {
      final String key1 = "6";

      // 1. Put with Memcached
      Future<Boolean> f = cacheFactory.getMemcachedClient().set(key1, 0, "v1");
      assertTrue(f.get(60, TimeUnit.SECONDS));
      CASValue oldValue = cacheFactory.getMemcachedClient().gets(key1);

      // 2. Replace with Hot Rod
      VersionedValue versioned = cacheFactory.getHotRodCache().getVersioned(key1);
      assertTrue(cacheFactory.getHotRodCache().replaceWithVersion(key1, "v2", versioned.getVersion()));

      // 4. Get with Memcached and verify value/CAS
      CASValue newValue = cacheFactory.getMemcachedClient().gets(key1);
      assertEquals("v2", newValue.getValue());
      assertTrue("The version (CAS) should have changed", oldValue.getCas() != newValue.getCas());
   }

   public void testEmbeddedHotRodReplaceMemcachedCASTest() throws Exception {
      final String key1 = "7";

      // 1. Put with Memcached
      Future<Boolean> f = cacheFactory.getMemcachedClient().set(key1, 0, "v1");
      assertTrue(f.get(60, TimeUnit.SECONDS));
      CASValue oldValue = cacheFactory.getMemcachedClient().gets(key1);

      // 2. Replace with Hot Rod
      VersionedValue versioned = cacheFactory.getHotRodCache().getVersioned(key1);
      assertTrue(cacheFactory.getHotRodCache().replaceWithVersion(key1, "v2", versioned.getVersion()));

      // 3. Replace with Embedded
      assertTrue(cacheFactory.getEmbeddedCache().replace(key1, "v2", "v3"));

      // 4. Get with Memcached and verify value/CAS
      CASValue newValue = cacheFactory.getMemcachedClient().gets(key1);
      assertEquals("v3", newValue.getValue());
      assertTrue("The version (CAS) should have changed", oldValue.getCas() != newValue.getCas());
   }

   static class SpyMemcachedCompatibleMarshaller extends AbstractMarshaller {

      private final Transcoder<Object> transcoder = new SerializingTranscoder();

      @Override
      protected ByteBuffer objectToBuffer(Object o, int estimatedSize) {
         CachedData encoded = transcoder.encode(o);
         return new ByteBufferImpl(encoded.getData(), 0, encoded.getData().length);
      }

      @Override
      public Object objectFromByteBuffer(byte[] buf, int offset, int length) {
         return transcoder.decode(new CachedData(0, buf, length));
      }

      @Override
      public boolean isMarshallable(Object o) throws Exception {
         try {
            transcoder.encode(o);
            return true;
         } catch (Throwable t) {
            return false;
         }
      }
   }

}
