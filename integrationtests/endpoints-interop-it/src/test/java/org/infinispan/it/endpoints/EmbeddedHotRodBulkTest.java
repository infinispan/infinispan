package org.infinispan.it.endpoints;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.util.Set;

import org.infinispan.Cache;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.commons.api.BasicCache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.test.AbstractInfinispanTest;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Test embedded caches and Hot Rod endpoints for operations that retrieve data in bulk,
 * i.e. keySet, entrySet...etc.
 *
 * @author Jiří Holuša
 * @since 6.0
 */
@Test(groups = "functional", testName = "it.endpoints.EmbeddedHotRodBulkTest")
public class EmbeddedHotRodBulkTest extends AbstractInfinispanTest {

   EndpointsCacheFactory<Integer, Integer> cacheFactory;

   @BeforeClass
   protected void setup() throws Exception {
      cacheFactory = new EndpointsCacheFactory.Builder<Integer, Integer>().withCacheMode(CacheMode.LOCAL).build();
   }

   @AfterClass
   protected void teardown() {
      EndpointsCacheFactory.killCacheFactories(cacheFactory);
   }

   private void populateCacheManager(BasicCache cache) {
      for (int i = 0; i < 100; i++) {
         cache.put(i, i);
      }
   }

   public void testEmbeddedPutHotRodKeySet() {
      Cache<Integer, Integer> embedded = cacheFactory.getEmbeddedCache();
      RemoteCache<Integer, Integer> remote = cacheFactory.getHotRodCache();

      populateCacheManager(embedded);

      Set<Integer> keySet = remote.keySet();
      assertEquals(100, keySet.size());

      for (int i = 0; i < 100; i++) {
         assertTrue(keySet.contains(i));
      }
   }

   public void testHotRodPutEmbeddedKeySet() {
      Cache<Integer, Integer> embedded = cacheFactory.getEmbeddedCache();
      RemoteCache<Integer, Integer> remote = cacheFactory.getHotRodCache();

      populateCacheManager(remote);

      Set<Integer> keySet = embedded.keySet();
      assertEquals(100, keySet.size());

      for (int i = 0; i < 100; i++) {
         assertTrue(keySet.contains(i));
      }
   }

}
