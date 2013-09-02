package org.infinispan.it.compatibility;

import org.infinispan.Cache;
import org.infinispan.client.hotrod.Flag;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.VersionedValue;
import org.infinispan.commons.api.BasicCache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.test.AbstractInfinispanTest;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Set;

import static org.testng.AssertJUnit.*;

/**
 * Test compatibility between embedded caches and Hot Rod endpoints for
 * operations that retrieve data in bulk, i.e. keySet, entrySet, getBulk...etc.
 *
 * @author Jiří Holuša
 * @since 6.0
 */
@Test(groups = "functional", testName = "it.compatibility.CompatibilityTest")
public class EmbeddedHotRodBulkTest extends AbstractInfinispanTest {

   CompatibilityCacheFactory<Integer, Integer> cacheFactory;

   @BeforeClass
   protected void setup() throws Exception {
      cacheFactory = new CompatibilityCacheFactory<Integer, Integer>(CacheMode.LOCAL).setup();
   }

   @AfterClass
   protected void teardown() {
      CompatibilityCacheFactory.killCacheFactories(cacheFactory);
   }

   private void populateCacheManager(BasicCache cache) {
      for (int i = 0; i < 100; i++) {
         cache.put(i, i);
      }
   }

   public void testEmbeddedPutHotRodGetBulk() {
      Cache<Integer, Integer> embedded = cacheFactory.getEmbeddedCache();
      RemoteCache<Integer, Integer> remote = cacheFactory.getHotRodCache();

      populateCacheManager(embedded);

      Map<Integer, Integer> get = remote.getBulk();
      assertEquals(100, get.size());

      for(int i = 0; i < 100; i++) {
         assertTrue(get.containsValue(i));
         assertTrue(get.containsKey(i));
      }
   }

   public void testEmbeddedPutHotRodGetBulkWithSize() {
      Cache<Integer, Integer> embedded = cacheFactory.getEmbeddedCache();
      RemoteCache<Integer, Integer> remote = cacheFactory.getHotRodCache();

      populateCacheManager(embedded);

      Map<Integer, Integer> get = remote.getBulk(50);
      assertEquals(50, get.size());
   }

   public void testEmbeddedPutHotRodKeySet() {
      Cache<Integer, Integer> embedded = cacheFactory.getEmbeddedCache();
      RemoteCache<Integer, Integer> remote = cacheFactory.getHotRodCache();

      populateCacheManager(embedded);

      Set<Integer> keySet = remote.keySet();
      assertEquals(100, keySet.size());

      for(int i = 0; i < 100; i++) {
         assertTrue(keySet.contains(i));
      }
   }

   public void testHotRodPutEmbeddedKeySet() {
      Cache<Integer, Integer> embedded = cacheFactory.getEmbeddedCache();
      RemoteCache<Integer, Integer> remote = cacheFactory.getHotRodCache();

      populateCacheManager(remote);

      Set<Integer> keySet = embedded.keySet();
      assertEquals(100, keySet.size());

      for(int i = 0; i < 100; i++) {
         assertTrue(keySet.contains(i));
      }
   }

}
