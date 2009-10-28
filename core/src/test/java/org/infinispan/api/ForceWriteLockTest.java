package org.infinispan.api;

import org.infinispan.AdvancedCache;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.ReadCommittedEntry;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.manager.CacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import javax.transaction.TransactionManager;

/**
 * @author Mircea.Markus@jboss.com
 */
@Test(groups = "functional", testName = "api.ForceWriteLockTest")
public class ForceWriteLockTest extends SingleCacheManagerTest {
   private TransactionManager tm;
   private AdvancedCache advancedCache;

   protected CacheManager createCacheManager() throws Exception {
      CacheManager cacheManager = TestCacheManagerFactory.createLocalCacheManager(true);
      advancedCache = cacheManager.getCache().getAdvancedCache();
      tm = TestingUtil.getTransactionManager(advancedCache);
      return cacheManager;
   }

   public void testWriteLockIsAcquired() throws Exception {
      advancedCache.put("k","v");
      assertNotLocked(advancedCache,"k");
      tm.begin();
      advancedCache.get("k", Flag.FORCE_WRITE_LOCK);

      InvocationContext ic = advancedCache.getInvocationContextContainer().getInvocationContext();
      CacheEntry cacheEntry = ic.getLookedUpEntries().get("k");
      assert (cacheEntry instanceof ReadCommittedEntry && cacheEntry.isChanged());

      assertLocked(advancedCache,"k");
      tm.commit();
      assertNotLocked(advancedCache,"k");
   }
}
