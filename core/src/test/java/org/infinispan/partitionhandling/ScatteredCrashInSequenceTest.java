package org.infinispan.partitionhandling;

import static org.infinispan.commons.test.Exceptions.expectException;
import static org.infinispan.test.TestingUtil.extractGlobalComponent;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.AssertJUnit.assertTrue;

import java.util.Arrays;
import java.util.HashSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.infinispan.Cache;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.topology.RebalancePhaseConfirmCommand;
import org.infinispan.commands.topology.RebalanceStartCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.distribution.MagicKey;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.inboundhandler.BlockingInboundInvocationHandler;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.transport.AbstractDelegatingTransport;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TransportFlags;
import org.jgroups.protocols.DISCARD;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "partitionhandling.ScatteredCrashInSequenceTest")
public class ScatteredCrashInSequenceTest extends BasePartitionHandlingTest {

   {
      cacheMode = CacheMode.SCATTERED_SYNC;
   }

   public void testSplit1() throws Exception {
      test(0, 1, 2, 3, true);
   }

   public void testSplit2() throws Exception {
      test(0, 2, 1, 3, true);
   }

   public void testSplit3() throws Exception {
      test(1, 0, 2, 3, true);
   }

   public void testSplit4() throws Exception {
      test(1, 2, 0, 3, true);
   }

   public void testSplit5() throws Exception {
      test(0, 1, 2, 3, false);
   }

   public void testSplit6() throws Exception {
      test(0, 2, 1, 3, false);
   }

   public void testSplit7() throws Exception {
      test(1, 0, 2, 3, false);
   }

   public void testSplit8() throws Exception {
      test(1, 2, 0, 3, false);
   }

   @Override
   protected ConfigurationBuilder cacheConfiguration() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.clustering().hash().numSegments(16);
      return builder;
   }

   @Override
   protected EmbeddedCacheManager addClusterEnabledCacheManager(ConfigurationBuilder builder, TransportFlags flags) {
      // The default global RPC timeout is 4 minutes, and if GET_STATUS returns SuspectException, we wait 1/20 of
      // this timeout before retrying. This is 12 seconds, which is longer than default eventually() timeout.
      // Therefore it is possible that if a partitioned node installs view with more members, and new view
      // fails the GET_STATUS, we don't become DEGRADED before the timeout expires.
      // Update: sometimes JGroups starts installing a non-MERGE view which triggers GET_STATUS which is called
      // with ClusterTopologyManagerImpl.clusterManagerLock locked. The request will be dropped/delayed, and
      // we have to wait until timeout to release the lock. The MERGE view won't be handled until this returns
      // due to the lock. Therefore setting to 5 seconds.
      GlobalConfigurationBuilder gcb = GlobalConfigurationBuilder.defaultClusteredBuilder();
      gcb.transport().distributedSyncTimeout(5, TimeUnit.SECONDS);
      return addClusterEnabledCacheManager(gcb, builder, flags);
   }

   private void test(int c1, int c2, int a1, int a2, boolean mergeInSplitOrder) throws Exception {
      Object[] keys = IntStream.range(0, numMembersInCluster).mapToObj(node -> {
         MagicKey key = new MagicKey(cache(node));
         cache(node).put(key, "v0");
         return key;
      }).toArray(MagicKey[]::new);


      DISCARD discard1 = TestingUtil.getDiscardForCache(manager(c1));
      DISCARD discard2 = TestingUtil.getDiscardForCache(manager(c2));

      Cache<?, ?> coordinator = c1 == 0 ? cache(1) : cache(0);

      // Block rebalance phase confirmations on the coordinator
      // Must replace the IIH first so it's updated in the transport
      BlockingInboundInvocationHandler blockedRebalanceConfirmations = BlockingInboundInvocationHandler.replace(coordinator.getCacheManager());
      blockedRebalanceConfirmations.blockBefore(RebalancePhaseConfirmCommand.class, c -> c.getCacheName().equals(getDefaultCacheName()));

      BlockRebalanceTransport controlledTransportCoord = replace(coordinator.getCacheManager());

      try {
         // Isolate c1, install view [c2, a1, a2] on the other nodes
         // The new view will trigger a rebalance
         BlockedRequest request = controlledTransportCoord.block(coordinator.getName());
         discard1.discardAll(true);
         Stream<Address> newMembers1 = manager(c2).getTransport().getMembers().stream()
                                                  .filter(n -> !n.equals(manager(c1).getAddress()));
         TestingUtil.installNewView(newMembers1, manager(c2), manager(a1), manager(a2));
         TestingUtil.installNewView(manager(c1));

         // Wait for the coordinator to start the rebalance for the default cache
         // The confirmation commands will be blocked on the coordinator
         assertTrue(request.commandBlocked.await(10, TimeUnit.SECONDS));
         controlledTransportCoord.stopBlocking();

         assertKeysAvailableForRead(cache(c2), keys);
         assertKeysAvailableForRead(cache(a1), keys);
         assertKeysAvailableForRead(cache(a2), keys);

         // The cache does not become degraded immediately, therefore if it was primary owner in previous
         // topology and it did not install new one/became degraded, it's still serving the old value
         eventuallyDegraded(cache(c1));
         assertKeysNotAvailableForRead(cache(c1), keys);

         // Isolate c2, install view [a1, a2] on the remaining nodes
         // After that, the default cache should become degraded on all nodes
         discard2.discardAll(true);
         Stream<Address> newMembers2 = manager(a1).getTransport().getMembers().stream()
                                                  .filter(n -> !n.equals(manager(c2).getAddress()));
         TestingUtil.installNewView(newMembers2, manager(a1), manager(a2));
         TestingUtil.installNewView(manager(c2));

         // Wait for and unblock 2 topology updates for both the default cache and the CONFIG cache
         eventuallyDegraded(cache(a1));
         eventuallyDegraded(cache(a2));
         eventuallyDegraded(cache(c2));
      } finally {
         blockedRebalanceConfirmations.stopBlocking();

         controlledTransportCoord.stopBlocking();
      }
      assertKeysNotAvailableForRead(cache(a1), keys);
      assertKeysNotAvailableForRead(cache(a2), keys);
      assertKeysNotAvailableForRead(cache(c1), keys);
      assertKeysNotAvailableForRead(cache(c2), keys);

      int m1 = mergeInSplitOrder ? c1 : c2;
      int m2 = mergeInSplitOrder ? c2 : c1;
      (mergeInSplitOrder ? discard1 : discard2).discardAll(false);
      TestingUtil.installNewView(manager(a1), manager(a2), manager(m1));

      eventuallyAvailable(cache(a1));
      eventuallyAvailable(cache(a2));
      eventuallyAvailable(cache(m1));
      eventuallyDegraded(cache(m2));

      assertKeysAvailableForRead(cache(m1), keys);
      assertKeysAvailableForRead(cache(a1), keys);
      assertKeysAvailableForRead(cache(a2), keys);
      assertKeysNotAvailableForRead(cache(m2), keys);

      (mergeInSplitOrder ? discard2 : discard1).discardAll(false);
      TestingUtil.installNewView(manager(a1), manager(a2), manager(m1), manager(m2));

      eventuallyAvailable(cache(m2));

      assertKeysAvailableForRead(cache(m2), keys);
      assertKeysAvailableForRead(cache(m1), keys);
      assertKeysAvailableForRead(cache(a1), keys);
      assertKeysAvailableForRead(cache(a2), keys);
   }

   private void eventuallyDegraded(Cache<?, ?> c) {
      eventually(() -> {
         AvailabilityMode currentMode = partitionHandlingManager(c).getAvailabilityMode();
         log.tracef("Current availability mode: %s", currentMode);
         return AvailabilityMode.DEGRADED_MODE.equals(currentMode);
      });
   }

   private void eventuallyAvailable(Cache<?, ?> c) {
      eventually(() -> {
         AvailabilityMode currentMode = partitionHandlingManager(c).getAvailabilityMode();
         log.tracef("Current availability mode: %s", currentMode);
         return AvailabilityMode.AVAILABLE.equals(currentMode);
      });
   }

   private void assertKeysAvailableForRead(Cache<?, ?> cache, Object... keys) {
      for (Object key : keys) {
         assertNotNull(cache.get(key), key.toString());
      }
      assertEquals(cache.getAdvancedCache().getAll(new HashSet<>(Arrays.asList(keys))).size(), keys.length);
   }

   private void assertKeysNotAvailableForRead(Cache<?, ?> cache, Object... keys) {
      for (Object key : keys) {
         expectException(AvailabilityException.class, () -> cache.get(key));
      }
      expectException(AvailabilityException.class,
                      () -> cache.getAdvancedCache().getAll(new HashSet<>(Arrays.asList(keys))));
   }

   private static BlockRebalanceTransport replace(EmbeddedCacheManager manager) {
      Transport transport = extractGlobalComponent(manager, Transport.class);
      if (transport instanceof BlockRebalanceTransport) {
         throw new IllegalStateException("One ControlledTransport per cache should be enough");
      }
      BlockRebalanceTransport controlledTransport = new BlockRebalanceTransport(transport);
      log.tracef("Installing BlockRebalanceTransport on %s", controlledTransport.getAddress());
      TestingUtil.replaceComponent(manager, Transport.class, controlledTransport, true);
      return controlledTransport;
   }

   @Scope(Scopes.GLOBAL)
   static class BlockRebalanceTransport extends AbstractDelegatingTransport {

      volatile BlockedRequest blockedRequest;

      BlockRebalanceTransport(Transport actual) {
         super(actual);
      }

      @Override
      public void sendToAll(ReplicableCommand rpcCommand, DeliverOrder deliverOrder) throws Exception {
         BlockedRequest current = blockedRequest;
         if (current != null && rpcCommand instanceof RebalanceStartCommand) {
            RebalanceStartCommand rebalanceStartCommand = (RebalanceStartCommand) rpcCommand;
            if (current.cacheName.equals(rebalanceStartCommand.getCacheName())) {
               blockedRequest.commandBlocked.countDown();
               blockedRequest.sendCommand.thenRun(() -> {
                  try {
                     super.sendToAll(rpcCommand, deliverOrder);
                  } catch (Exception e) {
                     log.error(e);
                  }
               });
            }
         }
         super.sendToAll(rpcCommand, deliverOrder);
      }

      BlockedRequest block(String cacheName) {
         assert blockedRequest == null;
         blockedRequest = new BlockedRequest(cacheName);
         return blockedRequest;
      }

      void stopBlocking() {
         BlockedRequest current = blockedRequest;
         blockedRequest = null;
         if (current != null) {
            current.sendCommand.complete(null);
         }
      }

      @Override
      public void start() {
         // Do nothing, the wrapped transport is already started
      }

      @Override
      public void stop() {
         stopBlocking();
         super.stop();
      }
   }

   private static class BlockedRequest {
      final String cacheName;
      final CountDownLatch commandBlocked;
      final CompletableFuture<Void> sendCommand;

      BlockedRequest(String cacheName) {
         this.cacheName = cacheName;
         commandBlocked = new CountDownLatch(1);
         sendCommand = new CompletableFuture<>();
      }
   }
}
