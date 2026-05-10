package org.infinispan.lock;

import static org.infinispan.test.TestingUtil.extractInterceptorChain;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.distribution.MagicKey;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.transaction.LockingMode;
import org.testng.annotations.Test;

/**
 * Checks that if a key's lock is the node where the transaction runs, then no remote RPC takes place.
 *
 * @author Mircea Markus
 * @since 5.1
 */
@Test (groups = "functional", testName = "lock.CheckNoRemoteCallForLocalKeyTest")
public class CheckNoRemoteCallForLocalKeyTest extends MultipleCacheManagersTest {

   protected CheckRemoteLockAcquiredOnlyOnceTest.ControlInterceptor controlInterceptor;
   protected CacheMode mode = CacheMode.REPL_SYNC;
   protected Object key;

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder c = getDefaultClusteredCacheConfig(mode, true);
      c.transaction().lockingMode(LockingMode.PESSIMISTIC);
      createCluster(c, 2);
      waitForClusterToForm();
      key = new MagicKey(cache(0));
      controlInterceptor = new CheckRemoteLockAcquiredOnlyOnceTest.ControlInterceptor();
      extractInterceptorChain(cache(1)).addInterceptor(controlInterceptor, 1);
   }

   public void testLocalPut() throws Exception {
      testLocalOperation(new CheckRemoteLockAcquiredOnlyOnceTest.CacheOperation() {
         @Override
         public void execute() {
            cache(0).put(key, "v");
         }
      });
   }

   public void testLocalRemove() throws Exception {
      testLocalOperation(new CheckRemoteLockAcquiredOnlyOnceTest.CacheOperation() {
         @Override
         public void execute() {
            cache(0).remove(key);
         }
      });
   }

   public void testLocalReplace() throws Exception {
      testLocalOperation(new CheckRemoteLockAcquiredOnlyOnceTest.CacheOperation() {
         @Override
         public void execute() {
            cache(0).replace(key, "", "");
         }
      });
   }

   public void testLocalLock() throws Exception {
      testLocalOperation(new CheckRemoteLockAcquiredOnlyOnceTest.CacheOperation() {
         @Override
         public void execute() {
            cache(0).getAdvancedCache().lock(key);
         }
      });
   }

   private void testLocalOperation(CheckRemoteLockAcquiredOnlyOnceTest.CacheOperation o) throws Exception {
      assertFalse(advancedCache(1).getRpcManager().getTransport().isCoordinator());
      assertTrue(advancedCache(0).getRpcManager().getTransport().isCoordinator());

      tm(0).begin();

      o.execute();

      assertTrue(lockManager(0).isLocked(key));
      assertFalse(lockManager(1).isLocked(key));

      assertEquals(0, controlInterceptor.remoteInvocations);
      tm(0).rollback();
   }
}
