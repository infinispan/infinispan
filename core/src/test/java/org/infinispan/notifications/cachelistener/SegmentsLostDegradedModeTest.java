package org.infinispan.notifications.cachelistener;

import static org.infinispan.test.TestingUtil.crashCacheManagers;
import static org.infinispan.test.TestingUtil.installNewView;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.infinispan.Cache;
import org.infinispan.commons.util.IntSet;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.SegmentsLost;
import org.infinispan.notifications.cachelistener.event.SegmentsLostEvent;
import org.infinispan.partitionhandling.AvailabilityMode;
import org.infinispan.partitionhandling.PartitionHandling;
import org.infinispan.partitionhandling.impl.PartitionHandlingManager;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestDataSCI;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.test.fwk.TransportFlags;
import org.testng.annotations.Test;

/**
 * A cache that enters degraded mode keeps its consistent hash, because the data comes back when the partitions
 * merge. No segment is lost, so no {@link SegmentsLost} notification must be raised.
 */
@Test(groups = "functional", testName = "notifications.cachelistener.SegmentsLostDegradedModeTest")
@CleanupAfterMethod
public class SegmentsLostDegradedModeTest extends MultipleCacheManagersTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      createClusteredCaches(3, TestDataSCI.INSTANCE,
            SegmentsLostListenerTest.configuration(PartitionHandling.DENY_READ_WRITES),
            new TransportFlags().withFD(true).withMerge(true));
   }

   public void testNoNotificationInDegradedMode() {
      Cache<Object, Object> survivor = cache(0);
      Object key = getKeyForCache(cache(1), cache(2));
      survivor.put(key, "value");

      SegmentsLostCollector listener = new SegmentsLostCollector();
      survivor.addListener(listener);

      crashCacheManagers(manager(1), manager(2));
      installNewView(manager(0));

      eventually("Cache did not enter degraded mode",
            () -> availabilityMode(survivor) == AvailabilityMode.DEGRADED_MODE);

      TestingUtil.sleepThread(TimeUnit.SECONDS.toMillis(1));
      assertTrue(listener.lostSegments.isEmpty(), "Unexpected events: " + listener.lostSegments);
   }

   private static AvailabilityMode availabilityMode(Cache<?, ?> cache) {
      return TestingUtil.extractComponent(cache, PartitionHandlingManager.class).getAvailabilityMode();
   }

   @Listener(clustered = true)
   public static class SegmentsLostCollector {
      final List<IntSet> lostSegments = Collections.synchronizedList(new ArrayList<>());

      @SegmentsLost
      public void onSegmentsLost(SegmentsLostEvent<Object, Object> event) {
         lostSegments.add(event.getSegments());
      }
   }
}
