package org.infinispan.stats.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.infinispan.stats.HotKeyTracker;
import org.infinispan.test.AbstractInfinispanTest;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "stats.impl.DisabledHotKeyTrackerTest")
public class DisabledHotKeyTrackerTest extends AbstractInfinispanTest {

   private static final int NUM_SEGMENTS = 4;

   public void testInRangeSegmentReportsZero() {
      HotKeyTracker tracker = HotKeyTracker.create(0, NUM_SEGMENTS);

      assertThat(tracker.segmentReads(NUM_SEGMENTS - 1)).isZero();
      assertThat(tracker.segmentWrites(0)).isZero();
   }

   public void testQueryingOutOfRangeSegmentIsRejected() {
      HotKeyTracker tracker = HotKeyTracker.create(0, NUM_SEGMENTS);

      // Same validation as the enabled tracker, so callers see one behaviour whether tracking is on or off.
      assertThatThrownBy(() -> tracker.segmentReads(NUM_SEGMENTS))
            .isInstanceOf(IllegalArgumentException.class);
      assertThatThrownBy(() -> tracker.segmentReads(-1))
            .isInstanceOf(IllegalArgumentException.class);
      assertThatThrownBy(() -> tracker.segmentWrites(NUM_SEGMENTS))
            .isInstanceOf(IllegalArgumentException.class);
      assertThatThrownBy(() -> tracker.segmentWrites(-1))
            .isInstanceOf(IllegalArgumentException.class);
   }

   public void testNegativeLimitRejected() {
      HotKeyTracker tracker = HotKeyTracker.create(0, NUM_SEGMENTS);

      // Same validation as the enabled tracker, so callers see one behaviour whether tracking is on or off.
      assertThatThrownBy(() -> tracker.getTopReads(-1))
            .as("negative read limit")
            .isInstanceOf(IllegalArgumentException.class);
      assertThatThrownBy(() -> tracker.getTopWrites(-1))
            .as("negative write limit")
            .isInstanceOf(IllegalArgumentException.class);

      assertThat(tracker.getTopReads(0)).as("zero read limit").isEmpty();
      assertThat(tracker.getTopWrites(0)).as("zero write limit").isEmpty();
   }
}
