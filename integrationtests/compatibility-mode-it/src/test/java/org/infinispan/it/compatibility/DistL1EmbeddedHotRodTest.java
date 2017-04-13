package org.infinispan.it.compatibility;

import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.getSplitIntKeyForServer;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.util.Arrays;
import java.util.List;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "it.compatibility.DistL1EmbeddedHotRodTest")
public class DistL1EmbeddedHotRodTest extends AbstractInfinispanTest {

   private static final int NUM_OWNERS = 1;

   private CompatibilityCacheFactory<Integer, String> cacheFactory1;
   private CompatibilityCacheFactory<Integer, String> cacheFactory2;

   @BeforeClass
   protected void setup() throws Exception {
      cacheFactory1 = new CompatibilityCacheFactory<Integer, String>(CacheMode.DIST_SYNC, NUM_OWNERS, true).setup();
      cacheFactory2 = new CompatibilityCacheFactory<Integer, String>(CacheMode.DIST_SYNC, NUM_OWNERS, true)
            .setup(cacheFactory1.getHotRodPort(), 100);

      List<Cache<Integer, String>> caches = Arrays.asList(cacheFactory1.getEmbeddedCache(), cacheFactory2.getEmbeddedCache());
      TestingUtil.blockUntilViewsReceived(30000, caches);
      TestingUtil.waitForStableTopology(caches);

      assertTrue(cacheFactory1.getHotRodCache().isEmpty());
      assertTrue(cacheFactory2.getHotRodCache().isEmpty());
   }

   @AfterClass
   protected void teardown() {
      CompatibilityCacheFactory.killCacheFactories(cacheFactory1, cacheFactory2);
   }

   public void testEmbeddedPutHotRodGetFromL1() {
      Cache<Integer, String> embedded1 = cacheFactory1.getEmbeddedCache();
      Cache<Integer, String> embedded2 = cacheFactory2.getEmbeddedCache();

      Integer key = getSplitIntKeyForServer(cacheFactory1.getHotrodServer(), cacheFactory2.getHotrodServer(), null);

      // Put it owner, forcing the remote node to get it from L1
      embedded1.put(key, "uno");

      // Should get it from L1
      assertEquals("uno", cacheFactory1.getHotRodCache().get(key));
      assertEquals("uno", cacheFactory1.getHotRodCache().get(key));
   }

}
