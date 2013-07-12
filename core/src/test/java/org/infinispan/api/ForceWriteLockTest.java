package org.infinispan.api;

import org.infinispan.AdvancedCache;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.ReadCommittedEntry;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import javax.transaction.TransactionManager;

/**
 * @author Mircea.Markus@jboss.com
 */
@Test(groups = "functional", testName = "api.ForceWriteLockTest", enabled = false,
      description = "mmarkus: email sent on 5 Sep '11 -> optimistic locking :: Flag.FORCE_WRITE_LOCK ")
public class ForceWriteLockTest extends SingleCacheManagerTest {
   private TransactionManager tm;
   private AdvancedCache advancedCache;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      EmbeddedCacheManager cacheManager = TestCacheManagerFactory.createCacheManager(true);
      advancedCache = cacheManager.getCache().getAdvancedCache();
      tm = TestingUtil.getTransactionManager(advancedCache);
      return cacheManager;
   }

   public void testWriteLockIsAcquired() throws Exception {
      advancedCache.put("k","v");
      assertNotLocked(advancedCache,"k");
      tm.begin();
      advancedCache.withFlags(Flag.FORCE_WRITE_LOCK).get("k");

      InvocationContext ic = advancedCache.getInvocationContextContainer().getInvocationContext(true);
      CacheEntry cacheEntry = ic.getLookedUpEntries().get("k");
      assert (cacheEntry instanceof ReadCommittedEntry && cacheEntry.isChanged());

      assertLocked(advancedCache,"k");
      tm.commit();
      assertNotLocked(advancedCache,"k");
   }
}
