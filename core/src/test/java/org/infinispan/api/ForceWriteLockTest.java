package org.infinispan.api;

import org.infinispan.AdvancedCache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.ReadCommittedEntry;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.LocalTransaction;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.TransactionCoordinator;
import org.infinispan.transaction.TransactionTable;
import org.testng.annotations.Test;

import javax.transaction.TransactionManager;

import static org.testng.AssertJUnit.assertTrue;

/**
 * @author Mircea.Markus@jboss.com
 */
@Test(groups = "functional", testName = "api.ForceWriteLockTest")
public class ForceWriteLockTest extends SingleCacheManagerTest {
   private TransactionManager tm;
   private AdvancedCache advancedCache;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder cacheConfiguration = TestCacheManagerFactory.getDefaultCacheConfiguration(true);
      cacheConfiguration.transaction().lockingMode(LockingMode.PESSIMISTIC);
      EmbeddedCacheManager cacheManager = TestCacheManagerFactory.createCacheManager(cacheConfiguration);
      advancedCache = cacheManager.getCache().getAdvancedCache();
      tm = TestingUtil.getTransactionManager(advancedCache);
      return cacheManager;
   }

   public void testWriteLockIsAcquired() throws Exception {
      advancedCache.put("k","v");
      assertNotLocked(advancedCache,"k");
      tm.begin();
      advancedCache.withFlags(Flag.FORCE_WRITE_LOCK).get("k");

      TransactionTable txTable = advancedCache.getComponentRegistry().getComponent(TransactionTable.class);
      LocalTransaction tx = txTable.getLocalTransaction(tm.getTransaction());
      assertTrue(tx.ownsLock("k"));
      assertLocked(advancedCache,"k");

      tm.commit();
      assertNotLocked(advancedCache,"k");
   }
}
