package org.infinispan.distribution;

import static org.testng.AssertJUnit.assertEquals;

import org.infinispan.atomic.TestDeltaAware;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.Test;

/**
 * JIRA: ISPN-7298
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
@Test(groups = "functional", testName = "distribution.DeltaAwareAsResponseTest")
public class DeltaAwareAsResponseTest extends MultipleCacheManagersTest {
   public void testOnPrimaryOwner() {
      doTest(new MagicKey(cache(0), cache(1)));
   }

   public void testOnBackupOwner() {
      doTest(new MagicKey(cache(1), cache(0)));
   }

   public void testOnNonOwner() {
      doTest(new MagicKey(cache(1), cache(2)));
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      createCluster(getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC), 3);
   }

   private void doTest(MagicKey key) {
      cache(0).put(key, new TestDeltaAware());
      Object response = cache(0).putIfAbsent(key, new TestDeltaAware());
      assertEquals(TestDeltaAware.class, response.getClass());
      ((TestDeltaAware) response).setFirstComponent("1");
      ((TestDeltaAware) response).setSecondComponent("2");

      response = cache(0).put(key, response);
      assertEquals(TestDeltaAware.class, response.getClass());
      assertEquals("1", ((TestDeltaAware) response).getFirstComponent());
      assertEquals("2", ((TestDeltaAware) response).getSecondComponent());

      response = cache(0).get(key);
      assertEquals(TestDeltaAware.class, response.getClass());
      assertEquals("1", ((TestDeltaAware) response).getFirstComponent());
      assertEquals("2", ((TestDeltaAware) response).getSecondComponent());
   }
}
