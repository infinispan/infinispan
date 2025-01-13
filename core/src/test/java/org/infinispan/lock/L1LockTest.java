package org.infinispan.lock;

import static org.testng.AssertJUnit.assertEquals;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestDataSCI;
import org.infinispan.transaction.lookup.EmbeddedTransactionManagerLookup;
import org.testng.annotations.Test;

/**
 * @author Mircea Markus &lt;mircea.markus@jboss.com&gt; (C) 2011 Red Hat Inc.
 * @since 5.1
 */
@Test(groups = "functional", testName = "lock.L1LockTest")
public class L1LockTest extends MultipleCacheManagersTest {
   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder config = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
      config.clustering().hash().numOwners(1).transaction().transactionManagerLookup(new EmbeddedTransactionManagerLookup());
      createCluster( TestDataSCI.INSTANCE, config, 2);
      waitForClusterToForm();
   }

   public void testConsistency() throws Exception {

      Object localKey = getKeyForCache(0);

      cache(0).put(localKey, "foo");
      assertNotLocked(localKey);

      assertEquals("foo", cache(0).get(localKey));
      assertNotLocked(localKey);

      log.trace("About to perform 2nd get...");
      Object o = cache(1).get(localKey);
      assertEquals("foo", cache(1).get(localKey));

      assertNotLocked(localKey);

      cache(0).put(localKey, "foo2");
      assertNotLocked(localKey);

      assertEquals("foo2", cache(0).get(localKey));
      assertEquals("foo2", cache(1).get(localKey));


      cache(1).put(localKey, "foo3");
      assertEquals("foo3", cache(0).get(localKey));
      assertEquals("foo3", cache(1).get(localKey));

   }
}
