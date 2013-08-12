package org.infinispan.distribution.rehash;

import org.infinispan.AdvancedCache;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.distribution.MagicKey;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.tx.dld.ControlledRpcManager;
import org.testng.annotations.Test;

import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;

/**
 * Tests data loss during state transfer when the primary owner of a key leaves during a put operation.
 * See https://issues.jboss.org/browse/ISPN-3366
 *
 * @author Dan Berindei
 */
@Test(groups = "functional", testName = "distribution.rehash.NonTxPrimaryOwnerLeavingTest")
@CleanupAfterMethod
public class NonTxPrimaryOwnerLeavingTest extends MultipleCacheManagersTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder c = new ConfigurationBuilder();
      c.clustering().cacheMode(CacheMode.DIST_SYNC);
      c.transaction().transactionMode(TransactionMode.NON_TRANSACTIONAL);

      addClusterEnabledCacheManager(c);
      addClusterEnabledCacheManager(c);
      waitForClusterToForm();
   }

   public void testPrimaryOwnerLeavingDuringPut() throws Exception {
      doTest(false);
   }

   public void testPrimaryOwnerLeavingDuringPutIfAbsent() throws Exception {
      doTest(true);
   }

   private void doTest(final boolean conditional) throws Exception {
      final AdvancedCache<Object, Object> cache0 = advancedCache(0);
      AdvancedCache<Object, Object> cache1 = advancedCache(1);

      // Block remote put commands invoked from cache0
      ControlledRpcManager crm = new ControlledRpcManager(cache0.getRpcManager());
      cache0.getComponentRegistry().registerComponent(crm, RpcManager.class);
      cache0.getComponentRegistry().rewire();
      crm.blockBefore(PutKeyValueCommand.class);

      // Try to put a key/value from cache0 with cache1 the primary owner
      final MagicKey key = new MagicKey(cache1);
      Future<Object> future = fork(new Callable<Object>() {
         @Override
         public Object call() throws Exception {
            return conditional ? cache0.putIfAbsent(key, "v") : cache0.put(key, "v");
         }
      });

      // After the put command was sent, kill cache1
      crm.waitForCommandToBlock();
      cache1.stop();

      // Now that cache1 is stopped, unblock the put command
      crm.stopBlocking();

      // Check that the put command didn't fail
      Object result = future.get(10, TimeUnit.SECONDS);
      assertNull(result);
      log.tracef("Put operation is done");

      // Check the value on the remaining node
      assertEquals("v", cache0.get(key));
   }
}