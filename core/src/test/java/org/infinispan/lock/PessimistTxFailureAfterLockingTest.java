package org.infinispan.lock;

import org.infinispan.Cache;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.control.LockControlCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.distribution.MagicKey;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.transaction.LockingMode;
import org.infinispan.util.AbstractControlledRpcManager;
import org.infinispan.util.concurrent.IsolationLevel;
import org.infinispan.util.concurrent.TimeoutException;
import org.testng.annotations.Test;

import java.util.Map;

import static org.testng.AssertJUnit.assertTrue;

/**
 * Test the failures after lock acquired for Pessimistic transactional caches.
 *
 * @author Pedro Ruivo
 * @since 6.0
 */
@Test(groups = "functional", testName = "lock.PessimistTxFailureAfterLockingTest")
@CleanupAfterMethod
public class PessimistTxFailureAfterLockingTest extends MultipleCacheManagersTest {

   /**
    * ISPN-3556
    */
   public void testReplyLostWithImplicitLocking() throws Exception {
      doTest(false);
   }

   /**
    * ISPN-3556
    */
   public void testReplyLostWithExplicitLocking() throws Exception {
      doTest(true);
   }

   private void doTest(boolean explicitLocking) throws Exception {
      final Object key = new MagicKey(cache(1), cache(2));
      replaceRpcManagerInCache(cache(0));

      boolean failed = false;
      tm(0).begin();
      try {
         if (explicitLocking) {
            cache(0).getAdvancedCache().lock(key);
         } else {
            cache(0).put(key, "value");
         }
      } catch (Exception e) {
         failed = true;
         //expected
      }
      tm(0).rollback();

      assertTrue("Expected an exception", failed);
      assertNoTransactions();
      assertNotLocked(cache(1), key);
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
      builder.locking()
            .isolationLevel(IsolationLevel.READ_COMMITTED); //read committed is enough
      builder.transaction()
            .lockingMode(LockingMode.PESSIMISTIC);
      builder.clustering().hash()
            .numOwners(2);
      createClusteredCaches(3, builder);
   }

   private void replaceRpcManagerInCache(Cache cache) {
      RpcManager rpcManager = TestingUtil.extractComponent(cache, RpcManager.class);
      TestControllerRpcManager testControllerRpcManager = new TestControllerRpcManager(rpcManager);
      TestingUtil.replaceComponent(cache, RpcManager.class, testControllerRpcManager, true);
   }

   /**
    * this RpcManager simulates a reply lost from LockControlCommand by throwing a TimeoutException. However, it is
    * expected the command to acquire the remote lock.
    */
   private class TestControllerRpcManager extends AbstractControlledRpcManager {

      public TestControllerRpcManager(RpcManager realOne) {
         super(realOne);
      }

      @Override
      protected Map<Address, Response> afterInvokeRemotely(ReplicableCommand command, Map<Address, Response> responseMap) {
         if (command instanceof LockControlCommand) {
            throw new TimeoutException("Exception expected!");
         }
         return responseMap;
      }
   }
}
