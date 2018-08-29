package org.infinispan.it.endpoints;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.io.IOException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.test.AbstractInfinispanTest;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Tests embedded and Memcached in a distributed clustered environment, using SpyMemcachedMarshaller.
 *
 * @author Martin Gencur
 * @since 6.0
 */
@Test(groups = "functional", testName = "it.endpoints.DistMemcachedEmbeddedTest")
public class DistMemcachedEmbeddedTest extends AbstractInfinispanTest {

   private final int numOwners = 1;
   //make sure the number of entries is big enough so that at least on entry
   //is stored on non-local node to the Memcached client
   private final int numEntries = 100;
   private final String cacheName = "memcachedCache";
   private EndpointsCacheFactory<String, Object> cacheFactory1;
   private EndpointsCacheFactory<String, Object> cacheFactory2;

   @BeforeClass
   protected void setup() throws Exception {
      cacheFactory1 = new EndpointsCacheFactory<String, Object>(cacheName, new SpyMemcachedMarshaller(),
            CacheMode.DIST_SYNC, numOwners, new MemcachedEncoder()).setup();
      cacheFactory2 = new EndpointsCacheFactory<String, Object>(cacheName, new SpyMemcachedMarshaller(),
            CacheMode.DIST_SYNC, numOwners, new MemcachedEncoder()).setup();
   }

   @AfterClass
   protected void teardown() {
      EndpointsCacheFactory.killCacheFactories(cacheFactory1, cacheFactory2);
   }

   public void testMemcachedPutEmbeddedGet() throws Exception {
      // 1. Put with Memcached
      for (int i = 0; i != numEntries; i++) {
         Future<Boolean> f = cacheFactory2.getMemcachedClient().set("k" + i, 0, "v" + i);
         assertTrue(f.get(60, TimeUnit.SECONDS));
      }

      // 2. Get with Embedded from a different node
      for (int i = 0; i != numEntries; i++) {
         assertEquals("v" + i, cacheFactory1.getEmbeddedCache().get("k" + i));
         cacheFactory1.getEmbeddedCache().remove("k" + i);
      }
   }

   public void testEmbeddedPutMemcachedGet() throws IOException {
      // 1. Put with Embedded
      for (int i = 0; i != numEntries; i++) {
         assertEquals(null, cacheFactory2.getEmbeddedCache().put("k" + i, "v" + i));
      }

      // 2. Get with Memcached from a different node
      for (int i = 0; i != numEntries; i++) {
         assertEquals("v" + i, cacheFactory1.getMemcachedClient().get("k" + i));
      }
   }

}
