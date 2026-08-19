package org.infinispan.conflict.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;

import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.IntSets;
import org.infinispan.test.AbstractInfinispanTest;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "conflict.impl.SegmentHashTrackerTest")
public class SegmentHashTrackerTest extends AbstractInfinispanTest {

   private static final int NUM_SEGMENTS = 4;
   private static final int BUCKET_COUNT = 8;

   private SegmentHashTracker tracker;
   private Marshaller marshaller;

   @BeforeMethod
   public void setUp() throws Exception {
      marshaller = mock(Marshaller.class);
      when(marshaller.objectToByteBuffer(any())).thenAnswer(invocation -> {
         Object arg = invocation.getArgument(0);
         return arg == null ? new byte[0] : arg.toString().getBytes();
      });

      tracker = new SegmentHashTracker();
      tracker.initForTest(marshaller, NUM_SEGMENTS, BUCKET_COUNT);
   }

   public void testInitialStateIsZero() {
      List<BucketHash> hashes = tracker.getBucketHashes(0);
      assertEquals(BUCKET_COUNT, hashes.size());
      for (BucketHash bh : hashes) {
         assertEquals(0L, bh.hash());
         assertEquals(0, bh.entryCount());
      }
   }

   public void testRecordInsert() {
      SegmentHasher.HashAndBucket hb = tracker.computeHashAndBucket("key1", "value1");
      tracker.recordInsert(0, hb.bucket(), hb.hash());

      List<BucketHash> hashes = tracker.getBucketHashes(0);
      BucketHash bucketHash = hashes.get(hb.bucket());
      assertEquals(hb.hash(), bucketHash.hash());
      assertEquals(1, bucketHash.entryCount());
   }

   public void testRecordRemoveUndoesInsert() {
      SegmentHasher.HashAndBucket hb = tracker.computeHashAndBucket("key1", "value1");
      tracker.recordInsert(0, hb.bucket(), hb.hash());
      tracker.recordRemove(0, hb.bucket(), hb.hash());

      List<BucketHash> hashes = tracker.getBucketHashes(0);
      BucketHash bucketHash = hashes.get(hb.bucket());
      assertEquals(0L, bucketHash.hash());
      assertEquals(0, bucketHash.entryCount());
   }

   public void testRecordUpdate() {
      SegmentHasher.HashAndBucket oldHb = tracker.computeHashAndBucket("key1", "value1");
      tracker.recordInsert(0, oldHb.bucket(), oldHb.hash());

      SegmentHasher.HashAndBucket newHb = tracker.computeHashAndBucket("key1", "value2");
      tracker.recordUpdate(0, oldHb.bucket(), oldHb.hash(), newHb.hash());

      List<BucketHash> hashes = tracker.getBucketHashes(0);
      BucketHash bucketHash = hashes.get(oldHb.bucket());
      assertEquals(newHb.hash(), bucketHash.hash());
      assertEquals(1, bucketHash.entryCount());
   }

   public void testXorOrderIndependence() {
      SegmentHasher.HashAndBucket hb1 = tracker.computeHashAndBucket("key1", "value1");
      SegmentHasher.HashAndBucket hb2 = tracker.computeHashAndBucket("key2", "value2");

      tracker.recordInsert(0, hb1.bucket(), hb1.hash());
      tracker.recordInsert(0, hb2.bucket(), hb2.hash());
      List<BucketHash> hashesOrder1 = tracker.getBucketHashes(0);

      tracker.resetSegment(0);
      tracker.recordInsert(0, hb2.bucket(), hb2.hash());
      tracker.recordInsert(0, hb1.bucket(), hb1.hash());
      List<BucketHash> hashesOrder2 = tracker.getBucketHashes(0);

      for (int b = 0; b < BUCKET_COUNT; b++) {
         assertTrue(hashesOrder1.get(b).equals(hashesOrder2.get(b)));
      }
   }

   public void testResetSegment() {
      SegmentHasher.HashAndBucket hb = tracker.computeHashAndBucket("key1", "value1");
      tracker.recordInsert(0, hb.bucket(), hb.hash());
      tracker.recordInsert(1, hb.bucket(), hb.hash());

      tracker.resetSegment(0);

      List<BucketHash> hashes0 = tracker.getBucketHashes(0);
      for (BucketHash bh : hashes0) {
         assertEquals(0L, bh.hash());
         assertEquals(0, bh.entryCount());
      }

      BucketHash bh1 = tracker.getBucketHashes(1).get(hb.bucket());
      assertEquals(hb.hash(), bh1.hash());
      assertEquals(1, bh1.entryCount());
   }

   public void testResetAllSegments() {
      SegmentHasher.HashAndBucket hb = tracker.computeHashAndBucket("key1", "value1");
      for (int s = 0; s < NUM_SEGMENTS; s++) {
         tracker.recordInsert(s, hb.bucket(), hb.hash());
      }

      tracker.resetAllSegments();

      for (int s = 0; s < NUM_SEGMENTS; s++) {
         for (BucketHash bh : tracker.getBucketHashes(s)) {
            assertEquals(0L, bh.hash());
            assertEquals(0, bh.entryCount());
         }
      }
   }

   public void testGetAllBucketHashes() {
      SegmentHasher.HashAndBucket hb = tracker.computeHashAndBucket("key1", "value1");
      tracker.recordInsert(0, hb.bucket(), hb.hash());
      tracker.recordInsert(2, hb.bucket(), hb.hash());

      IntSet segments = IntSets.mutableEmptySet(NUM_SEGMENTS);
      segments.set(0);
      segments.set(2);
      List<BucketHash> all = tracker.getAllBucketHashes(segments);
      assertEquals(2 * BUCKET_COUNT, all.size());

      int countNonZero = 0;
      for (BucketHash bh : all) {
         if (bh.hash() != 0L) countNonZero++;
      }
      assertEquals(2, countNonZero);
   }

   public void testMultipleInsertsAndRemoves() {
      SegmentHasher.HashAndBucket hb1 = tracker.computeHashAndBucket("key1", "value1");
      SegmentHasher.HashAndBucket hb2 = tracker.computeHashAndBucket("key2", "value2");
      SegmentHasher.HashAndBucket hb3 = tracker.computeHashAndBucket("key3", "value3");

      tracker.recordInsert(0, hb1.bucket(), hb1.hash());
      tracker.recordInsert(0, hb2.bucket(), hb2.hash());
      tracker.recordInsert(0, hb3.bucket(), hb3.hash());

      tracker.recordRemove(0, hb2.bucket(), hb2.hash());

      SegmentHashTracker tracker2 = new SegmentHashTracker();
      tracker2.initForTest(marshaller, NUM_SEGMENTS, BUCKET_COUNT);
      tracker2.recordInsert(0, hb1.bucket(), hb1.hash());
      tracker2.recordInsert(0, hb3.bucket(), hb3.hash());

      List<BucketHash> hashes1 = tracker.getBucketHashes(0);
      List<BucketHash> hashes2 = tracker2.getBucketHashes(0);

      for (int b = 0; b < BUCKET_COUNT; b++) {
         assertTrue(hashes1.get(b).equals(hashes2.get(b)));
      }
   }

   public void testUpdateEquivalentToRemoveAndInsert() {
      SegmentHasher.HashAndBucket oldHb = tracker.computeHashAndBucket("key1", "oldValue");
      SegmentHasher.HashAndBucket newHb = tracker.computeHashAndBucket("key1", "newValue");

      tracker.recordInsert(0, oldHb.bucket(), oldHb.hash());
      tracker.recordUpdate(0, oldHb.bucket(), oldHb.hash(), newHb.hash());

      SegmentHashTracker tracker2 = new SegmentHashTracker();
      tracker2.initForTest(marshaller, NUM_SEGMENTS, BUCKET_COUNT);
      tracker2.recordInsert(0, oldHb.bucket(), oldHb.hash());
      tracker2.recordRemove(0, oldHb.bucket(), oldHb.hash());
      tracker2.recordInsert(0, oldHb.bucket(), newHb.hash());

      List<BucketHash> hashes1 = tracker.getBucketHashes(0);
      List<BucketHash> hashes2 = tracker2.getBucketHashes(0);

      for (int b = 0; b < BUCKET_COUNT; b++) {
         assertTrue(hashes1.get(b).equals(hashes2.get(b)));
      }
   }

   public void testConsistencyWithSegmentHasher() throws Exception {
      long trackerHash = SegmentHasher.computeEntryHash("key1", "value1", marshaller);
      SegmentHasher.HashAndBucket hb = tracker.computeHashAndBucket("key1", "value1");
      assertEquals(trackerHash, hb.hash());
   }

   public void testBucketCountAccessor() {
      assertEquals(BUCKET_COUNT, tracker.bucketCount());
   }

   public void testEnabledState() {
      assertTrue(tracker.isEnabled());
   }

   public void testDisabledTrackerReturnsFalse() {
      SegmentHashTracker disabled = new SegmentHashTracker();
      assertFalse(disabled.isEnabled());
   }
}
