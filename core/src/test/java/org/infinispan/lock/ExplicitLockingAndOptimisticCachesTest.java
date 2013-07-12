package org.infinispan.lock;

import org.infinispan.commons.CacheException;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.context.Flag;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.LockingMode;
import org.testng.annotations.Test;

import java.util.concurrent.Callable;

import static org.infinispan.test.TestingUtil.withTx;

/**
 * @author Mircea Markus
 * @since 5.1
 */
@Test (groups = "functional", testName = "lock.ExplicitLockingAndOptimisticCachesTest")
public class ExplicitLockingAndOptimisticCachesTest extends SingleCacheManagerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      final ConfigurationBuilder c = getDefaultStandaloneCacheConfig(true);
      c.transaction().lockingMode(LockingMode.OPTIMISTIC);
      return TestCacheManagerFactory.createCacheManager(c);
   }

   public void testExplicitLockingNotWorkingWithOptimisticCaches() throws Exception {
      // Also provide guarantees that the transaction will come to an end
      withTx(tm(), new Callable<Object>() {
         @Override
         public Object call() throws Exception {
            try {
               cache.getAdvancedCache().lock("a");
               assert false;
            } catch (CacheException e) {
               // expected
            }
            return null;
         }
      });
   }

   public void testExplicitLockingOptimisticCachesFailSilent() throws Exception {
      // Also provide guarantees that the transaction will come to an end
      withTx(tm(), new Callable<Object>() {
         @Override
         public Object call() throws Exception {
            try {
               cache.getAdvancedCache().withFlags(Flag.FAIL_SILENTLY).lock("a");
               assert false : "Should be throwing an exception in spite of fail silent";
            } catch (CacheException e) {
               // expected
            }
            return null;
         }
      });
   }

}
