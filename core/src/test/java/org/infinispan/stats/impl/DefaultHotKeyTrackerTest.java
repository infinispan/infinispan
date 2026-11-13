package org.infinispan.stats.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.stat.HeavyKeeper;
import org.infinispan.test.AbstractInfinispanTest;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "stats.impl.DefaultHotKeyTrackerTest")
public class DefaultHotKeyTrackerTest extends AbstractInfinispanTest {

   private static final int K = 10;
   private static final int NUM_SEGMENTS = 4;

   public void testOutOfRangeSegmentStillTracksKey() {
      DefaultHotKeyTracker tracker = new DefaultHotKeyTracker(K, NUM_SEGMENTS);

      // Neither call may fail the cache operation that records the access.
      tracker.recordRead("read-key", NUM_SEGMENTS);
      tracker.recordRead("read-key", -1);
      tracker.recordWrite("write-key", NUM_SEGMENTS);
      tracker.recordWrite("write-key", -1);

      List<HeavyKeeper.KeyFrequency<Object>> reads = tracker.getTopReads(K);
      List<HeavyKeeper.KeyFrequency<Object>> writes = tracker.getTopWrites(K);
      assertThat(reads).extracting(HeavyKeeper.KeyFrequency::key).containsExactly("read-key");
      assertThat(writes).extracting(HeavyKeeper.KeyFrequency::key).containsExactly("write-key");

      // No valid segment received the out-of-range accesses.
      assertThat(tracker.totalReads()).isZero();
      assertThat(tracker.totalWrites()).isZero();
   }

   public void testInRangeSegmentIsCounted() {
      DefaultHotKeyTracker tracker = new DefaultHotKeyTracker(K, NUM_SEGMENTS);

      tracker.recordRead("k", NUM_SEGMENTS - 1);
      tracker.recordWrite("k", 0);

      assertThat(tracker.segmentReads(NUM_SEGMENTS - 1)).isEqualTo(1);
      assertThat(tracker.segmentWrites(0)).isEqualTo(1);
      assertThat(tracker.totalReads()).isEqualTo(1);
      assertThat(tracker.totalWrites()).isEqualTo(1);
   }

   public void testTopKOutsideConfigurationLimitsRejected() {
      // The tracker can be created without going through the configuration, so it enforces the same limits.
      assertThatThrownBy(() -> new DefaultHotKeyTracker(513, NUM_SEGMENTS))
            .isInstanceOf(CacheConfigurationException.class);
      assertThatThrownBy(() -> new DefaultHotKeyTracker(0, NUM_SEGMENTS))
            .isInstanceOf(CacheConfigurationException.class);
   }

   public void testLargestTopKTracksThatManyKeys() {
      DefaultHotKeyTracker tracker = new DefaultHotKeyTracker(512, NUM_SEGMENTS);

      for (int i = 0; i < 512; i++) {
         tracker.recordRead("key-" + i, 0);
      }

      assertThat(tracker.getTopReads(512)).hasSize(512);
   }

   public void testQueryingOutOfRangeSegmentIsRejected() {
      DefaultHotKeyTracker tracker = new DefaultHotKeyTracker(K, NUM_SEGMENTS);

      assertThatThrownBy(() -> tracker.segmentReads(NUM_SEGMENTS))
            .isInstanceOf(IllegalArgumentException.class);
      assertThatThrownBy(() -> tracker.segmentReads(-1))
            .isInstanceOf(IllegalArgumentException.class);
      assertThatThrownBy(() -> tracker.segmentWrites(NUM_SEGMENTS))
            .isInstanceOf(IllegalArgumentException.class);
      assertThatThrownBy(() -> tracker.segmentWrites(-1))
            .isInstanceOf(IllegalArgumentException.class);
   }
}
