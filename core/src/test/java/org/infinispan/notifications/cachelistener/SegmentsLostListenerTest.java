package org.infinispan.notifications.cachelistener;

import static org.infinispan.test.TestingUtil.crashCacheManagers;
import static org.infinispan.test.TestingUtil.installNewView;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.infinispan.Cache;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.IntSets;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.notifications.IncorrectListenerException;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryCreated;
import org.infinispan.notifications.cachelistener.annotation.SegmentsLost;
import org.infinispan.notifications.cachelistener.event.CacheEntryCreatedEvent;
import org.infinispan.notifications.cachelistener.event.Event;
import org.infinispan.notifications.cachelistener.event.SegmentsLostEvent;
import org.infinispan.partitionhandling.PartitionHandling;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestDataSCI;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.test.fwk.TransportFlags;
import org.infinispan.topology.CacheTopology;
import org.testng.annotations.Test;

/**
 * Tests that {@link SegmentsLost} listeners are notified when all the owners of one or more segments leave the
 * cluster, and that they are not notified when the data is still reachable.
 */
@Test(groups = "functional", testName = "notifications.cachelistener.SegmentsLostListenerTest")
@CleanupAfterMethod
public class SegmentsLostListenerTest extends MultipleCacheManagersTest {

   private static final int NUM_SEGMENTS = 64;
   private static final String VALUE = "value";
   private static final String SINGLE_OWNER_CACHE = "single-owner";
   private static final String SHARED_STORE_CACHE = "shared-store";
   private static final String PRIVATE_STORE_CACHE = "private-store";

   @Override
   protected void createCacheManagers() throws Throwable {
      createClusteredCaches(3, TestDataSCI.INSTANCE, configuration(PartitionHandling.ALLOW_READ_WRITES),
            new TransportFlags().withFD(true).withMerge(true));
   }

   static ConfigurationBuilder configuration(PartitionHandling partitionHandling) {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.clustering().cacheMode(CacheMode.DIST_SYNC)
            .hash().numOwners(2).numSegments(NUM_SEGMENTS);
      builder.clustering().partitionHandling().whenSplit(partitionHandling);
      return builder;
   }

   /**
    * The scenario from the feature request: the only two owners of a segment crash at the same time, and the
    * clustered listener registered on the surviving node is told which segments went away.
    */
   public void testClusteredListenerNotifiedWhenAllOwnersCrash() {
      Cache<Object, Object> survivor = cache(0);
      Object key = getKeyForCache(cache(1), cache(2));
      int keySegment = segmentOf(survivor, key);

      ClusteredSegmentsLostListener clusteredListener = new ClusteredSegmentsLostListener();
      LocalSegmentsLostListener localListener = new LocalSegmentsLostListener();
      survivor.addListener(clusteredListener);
      survivor.addListener(localListener);

      survivor.put(key, VALUE);
      eventually("Clustered listener did not receive the create event",
            () -> clusteredListener.createdKeys.contains(key));
      assertEquals(VALUE, survivor.get(key));

      IntSet expectedLostSegments = segmentsOwnedOnlyBy(survivor, address(1), address(2));
      assertTrue(expectedLostSegments.contains(keySegment),
            "Segment " + keySegment + " should be owned only by the crashing nodes, lost segments are " +
                  expectedLostSegments);

      crashCacheManagers(manager(1), manager(2));
      installNewView(manager(0));
      TestingUtil.waitForNoRebalance(survivor);

      // Both the clustered and the plain listener are notified, each segment exactly once
      assertLostSegments(clusteredListener, expectedLostSegments);
      assertLostSegments(localListener, expectedLostSegments);

      // The data really is gone, and no remove event was raised for it
      assertNull(survivor.get(key));
      assertFalse(survivor.containsKey(key));
   }

   /**
    * Every surviving node detects the loss on its own, but a clustered listener must still be told only once:
    * the notification is never collected from the other nodes.
    */
   public void testClusteredListenerNotifiedOnlyOnceWhenSeveralNodesSurvive() {
      addClusterEnabledCacheManager(TestDataSCI.INSTANCE, configuration(PartitionHandling.ALLOW_READ_WRITES),
            new TransportFlags().withFD(true).withMerge(true));
      waitForClusterToForm();

      // Not a MagicKey: that would need a segment whose primary is node 2 *and* backup is node 3, which the
      // consistent hash does not have to produce. Any key landing in a segment owned only by the two of them does.
      IntSet expectedLostSegments = segmentsOwnedOnlyBy(cache(0), address(2), address(3));
      Object key = keyInSegments(cache(0), expectedLostSegments);
      cache(0).put(key, VALUE);

      ClusteredSegmentsLostListener clusteredListener = new ClusteredSegmentsLostListener();
      LocalSegmentsLostListener listenerOnNode0 = new LocalSegmentsLostListener();
      LocalSegmentsLostListener listenerOnNode1 = new LocalSegmentsLostListener();
      cache(0).addListener(clusteredListener);
      cache(0).addListener(listenerOnNode0);
      cache(1).addListener(listenerOnNode1);

      crashCacheManagers(manager(2), manager(3));
      installNewView(manager(0), manager(1));
      TestingUtil.waitForNoRebalance(cache(0), cache(1));

      assertLostSegments(clusteredListener, expectedLostSegments);
      assertLostSegments(listenerOnNode0, expectedLostSegments);
      assertLostSegments(listenerOnNode1, expectedLostSegments);
      assertNull(cache(0).get(key));
   }

   /**
    * With a single owner per segment, losing any node loses data.
    */
   public void testSingleOwnerCacheLosesSegmentsWhenOwnerCrashes() {
      ConfigurationBuilder singleOwner = configuration(PartitionHandling.ALLOW_READ_WRITES);
      singleOwner.clustering().hash().numOwners(1);
      defineConfigurationOnAllManagers(SINGLE_OWNER_CACHE, singleOwner);
      waitForClusterToForm(SINGLE_OWNER_CACHE);

      Cache<Object, Object> survivor = cache(0, SINGLE_OWNER_CACHE);
      Object key = getKeyForCache(cache(2, SINGLE_OWNER_CACHE));
      survivor.put(key, VALUE);

      ClusteredSegmentsLostListener listener = new ClusteredSegmentsLostListener();
      survivor.addListener(listener);

      IntSet expectedLostSegments = segmentsOwnedOnlyBy(survivor, address(2));
      assertTrue(expectedLostSegments.contains(segmentOf(survivor, key)));

      crashCacheManagers(manager(2));
      installNewView(manager(0), manager(1));
      TestingUtil.waitForNoRebalance(cache(0, SINGLE_OWNER_CACHE), cache(1, SINGLE_OWNER_CACHE));

      assertLostSegments(listener, expectedLostSegments);
      assertNull(survivor.get(key));
   }

   /**
    * A shared store survives the nodes that wrote to it, so the new owners can still read the entries and nothing
    * was lost.
    */
   public void testNoNotificationWithSharedStore() {
      ConfigurationBuilder builder = configuration(PartitionHandling.ALLOW_READ_WRITES);
      builder.persistence().addStore(DummyInMemoryStoreConfigurationBuilder.class)
            .shared(true).storeName(SHARED_STORE_CACHE);
      defineConfigurationOnAllManagers(SHARED_STORE_CACHE, builder);
      waitForClusterToForm(SHARED_STORE_CACHE);

      Cache<Object, Object> survivor = cache(0, SHARED_STORE_CACHE);
      Object key = keyInSegments(survivor, segmentsOwnedOnlyBy(survivor, address(1), address(2)));
      survivor.put(key, VALUE);

      ClusteredSegmentsLostListener listener = new ClusteredSegmentsLostListener();
      survivor.addListener(listener);

      crashCacheManagers(manager(1), manager(2));
      installNewView(manager(0));
      TestingUtil.waitForNoRebalance(survivor);

      assertNoLostSegments(listener);
      // The premise: the entry really is still readable, from the shared store
      assertEquals(VALUE, survivor.get(key));
   }

   /**
    * A private store goes away with the node that owned it, so it must not suppress the notification.
    */
   public void testNotifiedWithPrivateStore() {
      ConfigurationBuilder builder = configuration(PartitionHandling.ALLOW_READ_WRITES);
      builder.persistence().addStore(DummyInMemoryStoreConfigurationBuilder.class).shared(false);
      defineConfigurationOnAllManagers(PRIVATE_STORE_CACHE, builder);
      waitForClusterToForm(PRIVATE_STORE_CACHE);

      Cache<Object, Object> survivor = cache(0, PRIVATE_STORE_CACHE);
      IntSet expectedLostSegments = segmentsOwnedOnlyBy(survivor, address(1), address(2));
      Object key = keyInSegments(survivor, expectedLostSegments);
      survivor.put(key, VALUE);

      ClusteredSegmentsLostListener listener = new ClusteredSegmentsLostListener();
      survivor.addListener(listener);

      crashCacheManagers(manager(1), manager(2));
      installNewView(manager(0));
      TestingUtil.waitForNoRebalance(survivor);

      assertLostSegments(listener, expectedLostSegments);
      assertNull(survivor.get(key));
   }

   /**
    * One owner out of two is enough to keep every segment alive, so nothing must be reported.
    */
   public void testNoNotificationWhenOneOwnerSurvives() {
      Cache<Object, Object> survivor = cache(0);
      Object key = getKeyForCache(cache(1), cache(2));

      ClusteredSegmentsLostListener listener = new ClusteredSegmentsLostListener();
      survivor.addListener(listener);
      survivor.put(key, VALUE);

      crashCacheManagers(manager(2));
      installNewView(manager(0), manager(1));
      TestingUtil.waitForNoRebalance(cache(0), cache(1));

      assertNoLostSegments(listener);
      assertEquals(VALUE, survivor.get(key));
   }

   /**
    * A node joining the cluster neither loses data nor makes the existing nodes believe data was lost.
    */
   public void testNoNotificationOnJoin() {
      ClusteredSegmentsLostListener listener = new ClusteredSegmentsLostListener();
      cache(0).addListener(listener);
      cache(0).put(getKeyForCache(cache(1), cache(2)), VALUE);

      EmbeddedCacheManager joiner = addClusterEnabledCacheManager(TestDataSCI.INSTANCE,
            configuration(PartitionHandling.ALLOW_READ_WRITES), new TransportFlags().withFD(true).withMerge(true));
      ClusteredSegmentsLostListener joinerListener = new ClusteredSegmentsLostListener();
      joiner.getCache().addListener(joinerListener);
      waitForClusterToForm();

      assertNoLostSegments(listener);
      assertNoLostSegments(joinerListener);
   }

   /**
    * A clustered listener that also asks for the current state must not run the segment loss event through the
    * initial state transfer queue, which only understands entry events.
    */
   public void testClusteredListenerWithCurrentState() {
      Cache<Object, Object> survivor = cache(0);
      Object key = getKeyForCache(cache(1), cache(2));
      survivor.put(key, VALUE);

      CurrentStateSegmentsLostListener listener = new CurrentStateSegmentsLostListener();
      survivor.addListener(listener);
      eventually("Listener did not receive the initial state", () -> listener.createdKeys.contains(key));

      IntSet expectedLostSegments = segmentsOwnedOnlyBy(survivor, address(1), address(2));

      crashCacheManagers(manager(1), manager(2));
      installNewView(manager(0));
      TestingUtil.waitForNoRebalance(survivor);

      assertLostSegments(listener, expectedLostSegments);
   }

   /**
    * The event is delivered to a listener that only declares a {@link SegmentsLost} method, i.e. one that has
    * nothing to install on the other nodes.
    */
   public void testClusteredListenerWithoutEntryEvents() {
      Cache<Object, Object> survivor = cache(0);
      Object key = getKeyForCache(cache(1), cache(2));
      survivor.put(key, VALUE);

      SegmentsLostOnlyListener listener = new SegmentsLostOnlyListener();
      survivor.addListener(listener);

      IntSet expectedLostSegments = segmentsOwnedOnlyBy(survivor, address(1), address(2));

      crashCacheManagers(manager(1), manager(2));
      installNewView(manager(0));
      TestingUtil.waitForNoRebalance(survivor);

      eventually("No segment loss reported", () -> !listener.lostSegments.isEmpty());
      assertEquals(expectedLostSegments, listener.lostSegments.get(0));
      assertEquals(1, listener.lostSegments.size(), "Unexpected events: " + listener.lostSegments);
   }

   public void testRemovedListenerIsNotNotified() {
      Cache<Object, Object> survivor = cache(0);
      ClusteredSegmentsLostListener listener = new ClusteredSegmentsLostListener();
      survivor.addListener(listener);
      survivor.removeListener(listener);

      crashCacheManagers(manager(1), manager(2));
      installNewView(manager(0));
      TestingUtil.waitForNoRebalance(survivor);

      assertNoLostSegments(listener);
   }

   public void testInvalidListenerMethodIsRejected() {
      assertThrows(IncorrectListenerException.class, () -> cache(0).addListener(new InvalidSegmentsLostListener()));
   }

   /**
    * Asserts that the listener was told about exactly the expected segments, each of them exactly once, however many
    * topology updates it took.
    */
   private void assertLostSegments(SegmentsLostListener listener, IntSet expectedSegments) {
      eventually("No segment loss reported, expected " + expectedSegments,
            () -> reportedSegments(listener).equals(expectedSegments));

      int reportedWithDuplicates = listener.lostSegments.stream().mapToInt(IntSet::size).sum();
      assertEquals(expectedSegments.size(), reportedWithDuplicates,
            "A segment was reported more than once: " + listener.lostSegments);

      SegmentsLostEvent<Object, Object> event = listener.events.get(0);
      assertEquals(Event.Type.SEGMENTS_LOST, event.getType());
      assertFalse(event.isPre(), "Segment loss is only ever reported after the fact");
      assertNotNull(event.getCache());
      assertTrue(event.getNewTopologyId() > 0, "Unexpected topology id " + event.getNewTopologyId());
      assertFalse(event.getSegments().isEmpty(), "An empty event must never be raised");
      assertThrows(UnsupportedOperationException.class, () -> event.getSegments().set(NUM_SEGMENTS - 1));
   }

   private static IntSet reportedSegments(SegmentsLostListener listener) {
      IntSet all = IntSets.mutableEmptySet(NUM_SEGMENTS);
      for (IntSet segments : List.copyOf(listener.lostSegments)) {
         all.addAll(segments);
      }
      return all;
   }

   /**
    * The notification is raised at the tail of the topology update, so give it a chance to arrive before concluding
    * that nothing was reported.
    */
   private static void assertNoLostSegments(SegmentsLostListener listener) {
      TestingUtil.sleepThread(TimeUnit.SECONDS.toMillis(1));
      assertTrue(listener.lostSegments.isEmpty(), "Unexpected events: " + listener.lostSegments);
   }

   /**
    * @return a key that hashes to one of the given segments.
    */
   private static Object keyInSegments(Cache<?, ?> cache, IntSet segments) {
      assertFalse(segments.isEmpty(), "The consistent hash left no segment to lose");
      KeyPartitioner keyPartitioner = TestingUtil.extractComponent(cache, KeyPartitioner.class);
      for (int i = 0; i < 10_000; i++) {
         String key = "key-" + i;
         if (segments.contains(keyPartitioner.getSegment(key))) {
            return key;
         }
      }
      throw new IllegalStateException("No key found for segments " + segments);
   }

   private static int segmentOf(Cache<?, ?> cache, Object key) {
      return cache.getAdvancedCache().getDistributionManager().getCacheTopology().getSegment(key);
   }

   /**
    * @return the segments whose every owner is one of the given addresses.
    */
   private static IntSet segmentsOwnedOnlyBy(Cache<?, ?> cache, Address... addresses) {
      CacheTopology topology = cache.getAdvancedCache().getDistributionManager().getCacheTopology();
      ConsistentHash readCH = topology.getReadConsistentHash();
      List<Address> leavers = List.of(addresses);
      IntSet segments = IntSets.mutableEmptySet(readCH.getNumSegments());
      for (int segment = 0; segment < readCH.getNumSegments(); segment++) {
         if (leavers.containsAll(readCH.locateOwnersForSegment(segment))) {
            segments.set(segment);
         }
      }
      return segments;
   }

   public abstract static class SegmentsLostListener {
      final List<Object> createdKeys = Collections.synchronizedList(new ArrayList<>());
      final List<IntSet> lostSegments = Collections.synchronizedList(new ArrayList<>());
      final List<SegmentsLostEvent<Object, Object>> events = Collections.synchronizedList(new ArrayList<>());

      @CacheEntryCreated
      public void onCreated(CacheEntryCreatedEvent<Object, Object> event) {
         createdKeys.add(event.getKey());
      }

      @SegmentsLost
      public void onSegmentsLost(SegmentsLostEvent<Object, Object> event) {
         events.add(event);
         lostSegments.add(event.getSegments());
      }
   }

   @Listener(clustered = true)
   public static class ClusteredSegmentsLostListener extends SegmentsLostListener {
   }

   @Listener
   public static class LocalSegmentsLostListener extends SegmentsLostListener {
   }

   @Listener(clustered = true, includeCurrentState = true)
   public static class CurrentStateSegmentsLostListener extends SegmentsLostListener {
   }

   @Listener(clustered = true)
   public static class SegmentsLostOnlyListener {
      final List<IntSet> lostSegments = Collections.synchronizedList(new ArrayList<>());

      @SegmentsLost
      public void onSegmentsLost(SegmentsLostEvent<Object, Object> event) {
         lostSegments.add(event.getSegments());
      }
   }

   @Listener
   public static class InvalidSegmentsLostListener {
      @SegmentsLost
      public void onSegmentsLost(CacheEntryCreatedEvent<Object, Object> event) {
      }
   }
}
