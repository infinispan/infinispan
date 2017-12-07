package org.infinispan.scattered;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CompletionStage;

import org.infinispan.Cache;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.remote.RevokeBiasCommand;
import org.infinispan.configuration.cache.BiasAcquisition;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.distribution.MagicKey;
import org.infinispan.remoting.RemoteException;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.rpc.RpcOptions;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.ResponseCollector;
import org.infinispan.test.Exceptions;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.util.AbstractControlledRpcManager;
import org.infinispan.util.CountingRpcManager;
import org.infinispan.util.concurrent.CompletableFutures;
import org.infinispan.util.concurrent.TimeoutException;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "scattered.BiasRevocationTest")
public class BiasRevocationTest extends MultipleCacheManagersTest {
   private FailingRpcManager rpcManager0;
   private CountingRpcManager rpcManager2;

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.SCATTERED_SYNC, false);
      builder.clustering().biasAcquisition(BiasAcquisition.ON_WRITE).remoteTimeout(1000);
      createCluster(builder, 3);

      TestingUtil.wrapComponent(cache(0), RpcManager.class, rpcManager -> rpcManager0 = new FailingRpcManager(rpcManager));
      cache(1); // just touch to start it
      TestingUtil.wrapComponent(cache(2), RpcManager.class, rpcManager -> rpcManager2 = new CountingRpcManager(rpcManager));
   }

   protected static void put(Cache cache, Object key, Object value) {
      cache.put(key, value);
   }

   protected static void putAll(Cache cache, Object key, Object value) {
      cache.putAll(Collections.singletonMap(key, value));
   }

   public void testFailedRevocationDuringPutOnPrimaryThrowBefore() {
      testFailedRevocation(() -> rpcManager0.throwBefore = !rpcManager0.throwBefore, BiasRevocationTest::put, true);
   }

   public void testFailedRevocationDuringPutOnPrimaryThrowInFuture() {
      testFailedRevocation(() -> rpcManager0.throwInFuture = !rpcManager0.throwInFuture, BiasRevocationTest::put, true);
   }

   public void testFailedRevocationDuringPutAllOnPrimaryThrowBefore() {
      testFailedRevocation(() -> rpcManager0.throwBefore = !rpcManager0.throwBefore, BiasRevocationTest::putAll, true);
   }

   public void testFailedRevocationDuringPutAllOnPrimaryThrowInFuture() {
      testFailedRevocation(() -> rpcManager0.throwInFuture = !rpcManager0.throwInFuture, BiasRevocationTest::putAll, true);
   }

   public void testFailedRevocationDuringPutOnNonOwnerThrowBefore() {
      testFailedRevocation(() -> rpcManager0.throwBefore = !rpcManager0.throwBefore, BiasRevocationTest::put, false);
   }

   public void testFailedRevocationDuringPutOnNonOwnerThrowInFuture() {
      testFailedRevocation(() -> rpcManager0.throwInFuture = !rpcManager0.throwInFuture, BiasRevocationTest::put, false);
   }

   public void testFailedRevocationDuringPutAllOnNonOwnerThrowBefore() {
      testFailedRevocation(() -> rpcManager0.throwBefore = !rpcManager0.throwBefore, BiasRevocationTest::putAll, false);
   }

   public void testFailedRevocationDuringPutAllOnNonOwnerThrowInFuture() {
      testFailedRevocation(() -> rpcManager0.throwInFuture = !rpcManager0.throwInFuture, BiasRevocationTest::putAll, false);
   }

   protected void testFailedRevocation(Runnable switchFailure, Operation operation, boolean primary) {
      MagicKey key = new MagicKey(cache(0));
      cache(2).put(key, "v0");
      assertTrue(biasManager(2).hasLocalBias(key));
      assertEquals(Collections.singletonList(address(2)), biasManager(0).getRemoteBias(key));

      // Assert the read is local
      rpcManager2.resetStats();
      assertEquals("v0", cache(2).get(key));
      assertEquals(0, rpcManager2.clusterGet);
      assertEquals(0, rpcManager2.otherCount);

      switchFailure.run();
      if (primary) {
         Exceptions.expectException(RemoteException.class, () -> operation.apply(cache(0), key, "v1"));
      } else {
         Exceptions.expectException(TimeoutException.class, () -> operation.apply(cache(1), key, "v1"));
      }

      // The replication to backup does not fail; only the revocation. Since cache(1) will be the backup for primary
      // updates, the value will be properly updated there
      assertTrue(biasManager(2).hasLocalBias(key));
      assertEquals("v1", cache(0).get(key));
      assertEquals("v1", cache(1).get(key));
      assertEquals("v0", cache(2).get(key));

      switchFailure.run();
      assertEquals("v1", cache(1).put(key, "v2"));
      assertFalse(biasManager(2).hasLocalBias(key));
      assertEquals("v2", cache(2).get(key));
   }

   @AfterMethod
   public void resetFailures() {
      rpcManager0.throwBefore = false;
      rpcManager0.throwInFuture = false;
      caches().forEach(Cache::clear);
   }

   protected BiasManager biasManager(int index) {
      return cache(index).getAdvancedCache().getComponentRegistry().getComponent(BiasManager.class);
   }

   private interface Operation {
      void apply(Cache cache, Object key, Object value);
   }

   private class FailingRpcManager extends AbstractControlledRpcManager {
      public boolean throwBefore = false;
      public boolean throwInFuture = false;

      public FailingRpcManager(RpcManager realOne) {
         super(realOne);
      }

      @Override
      public <T> CompletionStage<T> invokeCommand(Collection<Address> targets, ReplicableCommand command,
                                                  ResponseCollector<T> collector, RpcOptions rpcOptions) {
         if (command instanceof RevokeBiasCommand) {
            if (throwBefore)
               throw new RemoteException("Induced", null);
            if (throwInFuture) {
               return CompletableFutures.completedExceptionFuture(new RemoteException("Induced", null));
            }
         }
         return super.invokeCommand(targets, command, collector, rpcOptions);
      }
   }
}
