package org.infinispan.partitionhandling;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.distribution.MagicKey;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.concurrent.StateSequencer;
import org.infinispan.test.fwk.TransportFlags;
import org.infinispan.topology.CacheTopologyControlCommand;
import org.infinispan.topology.ClusterTopologyManager;
import org.jgroups.protocols.DISCARD;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static org.infinispan.test.concurrent.StateSequencerUtil.advanceOnGlobalComponentMethod;
import static org.infinispan.test.concurrent.StateSequencerUtil.matchMethodCall;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.fail;

@Test(groups = "functional")
public class ScatteredCrashInSequenceTest extends BasePartitionHandlingTest {

   private Transport oldTransportCoord;
   private Transport oldTransportA1;

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
   protected EmbeddedCacheManager addClusterEnabledCacheManager(ConfigurationBuilder builder, TransportFlags flags) {
      // The default global RPC timeout is 4 minutes, and if GET_STATUS returns SuspectException, we wait 1/20 of
      // this timeout before retrying. This is 12 seconds, which is longer than default eventually() timeout.
      // Therefore it is possible that if a partitioned node installs view with more members, and new view
      // fails the GET_STATUS, we don't become DEGRADED before the timeout expires.
      GlobalConfigurationBuilder gcb = GlobalConfigurationBuilder.defaultClusteredBuilder();
      gcb.transport().distributedSyncTimeout(1, TimeUnit.MINUTES);
      return addClusterEnabledCacheManager(gcb, builder, flags);
   }

   private void test(int c1, int c2, int a1, int a2, boolean mergeInSplitOrder) throws Exception {
      MagicKey[] keys = IntStream.range(0, numMembersInCluster).mapToObj(node -> {
         MagicKey key = new MagicKey(cache(node));
         cache(node).put(key, "v0");
         return key;
      }).toArray(MagicKey[]::new);


      StateSequencer ss = new StateSequencer().logicalThread("main", "st_begin", "check", "new_topology", "st_end", "degraded");

      DISCARD discard1 = TestingUtil.getDiscardForCache(cache(c1));
      DISCARD discard2 = TestingUtil.getDiscardForCache(cache(c2));

      Cache coordinator = c1 == 0 ? cache(1) : cache(0);
      // This doesn't go through RpcManager, so we can't mock this and we can't mock ClusterTopologyManager either
      // as ClusterCacheStatus does not get rewired.
      oldTransportCoord = advanceOnGlobalComponentMethod(ss, coordinator.getCacheManager(), Transport.class,
         matchMethodCall("invokeRemotely")
            .withMatcher(1, new CacheTopologyControlCommandMatcher(CacheTopologyControlCommand.Type.REBALANCE_START))
            .matchCount(0).build()
      ).after("st_begin").getOriginalComponent();

      // We need to mock the other transport before replacing ClusterTopologyManager in case that coord == a1
      oldTransportA1 = advanceOnGlobalComponentMethod(ss, cache(a1).getCacheManager(), Transport.class,
         matchMethodCall("invokeRemotely")
            .withMatcher(1, new CacheTopologyControlCommandMatcher(CacheTopologyControlCommand.Type.CH_UPDATE))
            .afterState(ss, "check").matchCount(0).build()
      ).before("new_topology").getOriginalComponent();

      // Since now coordinator's ClusterTopologyManager does not get rewired if we replace another component
      advanceOnGlobalComponentMethod(ss, coordinator.getCacheManager(), ClusterTopologyManager.class,
         matchMethodCall("handleRebalanceCompleted").matchCount(0).build()
      ).before("st_end"); // this is meant just to block until we get to new_topology

      discard1.setDiscardAll(true);

      ss.enter("check");
      assertKeysAvailableForRead(cache(c2), keys);
      assertKeysAvailableForRead(cache(a1), keys);
      assertKeysAvailableForRead(cache(a2), keys);

      // The cache does not become degraded immediatelly, therefore if it was primary owner in previous
      // topology and it did not install new one/became degraded, it's still serving the old value
      eventuallyDegraded(cache(c1));
      assertKeysNotAvailableForRead(cache(c1), keys);
      ss.exit("check");

      discard2.setDiscardAll(true);

      ss.advance("degraded");

      eventuallyDegraded(cache(a1));
      eventuallyDegraded(cache(a2));
      eventuallyDegraded(cache(c2));

      assertKeysNotAvailableForRead(cache(a1), keys);
      assertKeysNotAvailableForRead(cache(a2), keys);
      assertKeysNotAvailableForRead(cache(c1), keys);
      assertKeysNotAvailableForRead(cache(c2), keys);

      int m1 = mergeInSplitOrder ? c1 : c2;
      int m2 = mergeInSplitOrder ? c2 : c1;
      (mergeInSplitOrder ? discard1 : discard2).setDiscardAll(false);
      TestingUtil.blockUntilViewsReceived(60000, cache(a1), cache(a2), cache(m1));

      eventuallyAvailable(cache(a1));
      eventuallyAvailable(cache(a2));
      eventuallyAvailable(cache(m1));
      eventuallyDegraded(cache(m2));

      assertKeysAvailableForRead(cache(m1), keys);
      assertKeysAvailableForRead(cache(a1), keys);
      assertKeysAvailableForRead(cache(a2), keys);
      assertKeysNotAvailableForRead(cache(m2), keys);

      (mergeInSplitOrder ? discard2 : discard1).setDiscardAll(false);
      TestingUtil.blockUntilViewsReceived(60000, cache(a1), cache(a2), cache(m1), cache(m2));

      eventuallyAvailable(cache(m2));

      assertKeysAvailableForRead(cache(m2), keys);
      assertKeysAvailableForRead(cache(m1), keys);
      assertKeysAvailableForRead(cache(a1), keys);
      assertKeysAvailableForRead(cache(a2), keys);
   }

   @AfterMethod
   @Override
   protected void clearContent() throws Throwable {
      // The transport won't be stopped automatically on test shutdown as the component registry won't invoke
      // stop methods on proxified instance.
      if (oldTransportCoord != null) oldTransportCoord.stop();
      oldTransportCoord = null;
      if (oldTransportA1 != null) oldTransportA1.stop();
      oldTransportA1 = null;
      super.clearContent();
   }

   private void eventuallyDegraded(Cache c) {
      eventually(() -> {
         AvailabilityMode currentMode = partitionHandlingManager(c).getAvailabilityMode();
         log.tracef("Current availability mode: %s", currentMode);
         return AvailabilityMode.DEGRADED_MODE.equals(currentMode);
      });
   }

   private void eventuallyAvailable(Cache c) {
      eventually(() -> {
         AvailabilityMode currentMode = partitionHandlingManager(c).getAvailabilityMode();
         log.tracef("Current availability mode: %s", currentMode);
         return AvailabilityMode.AVAILABLE.equals(currentMode);
      });
   }

   private void assertKeysAvailableForRead(Cache cache, Object... keys) {
      for (Object key : keys) {
         assertNotNull(cache.get(key), key.toString());
      }
   }

   private void assertKeysNotAvailableForRead(Cache cache, Object... keys) {
      for (Object key : keys) {
         try {
            cache.get(key);
            fail();
         } catch (AvailabilityException ae) {}
      }
   }

   private static class CacheTopologyControlCommandMatcher extends BaseMatcher<Object> {
      private final CacheTopologyControlCommand.Type type;

      private CacheTopologyControlCommandMatcher(CacheTopologyControlCommand.Type type) {
         this.type = type;
      }

      @Override
      public boolean matches(Object item) {
         return item instanceof CacheTopologyControlCommand && ((CacheTopologyControlCommand) item).getType() == type;
      }

      @Override
      public void describeTo(Description description) {
         description.appendText("is CacheTopologyControlCommand." + type);
      }
   }
}
