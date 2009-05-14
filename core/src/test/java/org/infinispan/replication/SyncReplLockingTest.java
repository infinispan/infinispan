/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.infinispan.replication;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;

import javax.transaction.TransactionManager;

import org.infinispan.Cache;
import org.infinispan.config.Configuration;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.transaction.lookup.DummyTransactionManagerLookup;
import org.infinispan.util.concurrent.locks.LockManager;
import org.jgroups.util.Util;
import org.testng.annotations.Test;

/**
 * @author <a href="mailto:manik@jboss.org">Manik Surtani (manik@jboss.org)</a>
 */
@Test(groups = "functional", testName = "replication.SyncReplLockingTest")
public class SyncReplLockingTest extends MultipleCacheManagersTest {
   Cache<String, String> cache1, cache2;
   String k = "key", v = "value";

   protected void createCacheManagers() throws Throwable {
      Configuration replSync = getDefaultClusteredConfig(Configuration.CacheMode.REPL_SYNC);
      replSync.setTransactionManagerLookupClass(DummyTransactionManagerLookup.class.getName());
      createClusteredCaches(2, "replSync", replSync);

      cache1 = manager(0).getCache("replSync");
      cache2 = manager(1).getCache("replSync");
   }

   public void testLockingWithExplicitUnlock() throws Exception {
      lockingWithExplicitUnlockHelper(false);
      lockingWithExplicitUnlockHelper(true);
   }

   public void testLocksReleasedWithoutExplicitUnlock() throws Exception {
      locksReleasedWithoutExplicitUnlockHelper(false);
      locksReleasedWithoutExplicitUnlockHelper(true);
   }

   private void lockingWithExplicitUnlockHelper(boolean lockPriorToPut) throws Exception {
      assertClusterSize("Should only be 2  caches in the cluster!!!", 2);

      assertNull("Should be null", cache1.get(k));
      assertNull("Should be null", cache2.get(k));

      String name = "Vladimir";
      TransactionManager mgr = TestingUtil.getTransactionManager(cache1);
      mgr.begin();
      if (lockPriorToPut)
         cache1.getAdvancedCache().lock(k);

      cache1.put(k, v);

      cache1.put(k, name);
      if (!lockPriorToPut)
         cache1.getAdvancedCache().lock(k);

      cache1.getAdvancedCache().unlock(k);
      mgr.commit();

      assertEquals(name, cache1.get(k));
      assertEquals("Should have replicated", name, cache2.get(k));

      cache2.remove(k);
      assert cache1.isEmpty();
      assert cache2.isEmpty();
   }

   private void locksReleasedWithoutExplicitUnlockHelper(boolean lockPriorToPut) throws Exception {
      assertClusterSize("Should only be 2  caches in the cluster!!!", 2);

      assertNull("Should be null", cache1.get(k));
      assertNull("Should be null", cache2.get(k));

      String name = "Infinispan";
      TransactionManager mgr = TestingUtil.getTransactionManager(cache1);
      mgr.begin();
      if (lockPriorToPut)
         cache1.getAdvancedCache().lock(k);
      cache1.put(k, name);
      if (!lockPriorToPut)
         cache1.getAdvancedCache().lock(k);
      mgr.commit();

      assertEquals(name, cache1.get(k));
      assertEquals("Should have replicated", name, cache2.get(k));

      cache2.remove(k);
      assert cache1.isEmpty();
      assert cache2.isEmpty();
   }

}
