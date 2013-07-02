package org.infinispan.lock;

import org.infinispan.config.Configuration;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.transaction.lookup.DummyTransactionManagerLookup;
import org.testng.annotations.Test;

import static junit.framework.Assert.assertEquals;

/**
 * @author Mircea Markus <mircea.markus@jboss.com> (C) 2011 Red Hat Inc.
 * @since 5.1
 */
@Test(groups = "functional", testName = "lock.L1LockTest")
public class L1LockTest extends MultipleCacheManagersTest {
   @Override
   protected void createCacheManagers() throws Throwable {
      Configuration config = getDefaultClusteredConfig(Configuration.CacheMode.DIST_SYNC, true);
      config.fluent().hash().numOwners(1).transaction().transactionManagerLookup(new DummyTransactionManagerLookup());
      createCluster(config, 2);
      waitForClusterToForm();
   }

   public void testConsistency() throws Exception {

      Object localKey = getKeyForCache(0);

      cache(0).put(localKey, "foo");
      assertNotLocked(localKey);

      assertEquals("foo", cache(0).get(localKey));
      assertNotLocked(localKey);

      log.trace("About to perform 2nd get...");
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
