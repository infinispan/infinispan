package org.infinispan.conflict.impl;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotSame;
import static org.testng.AssertJUnit.assertTrue;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.util.IntSets;
import org.infinispan.container.entries.ImmortalCacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.impl.InternalDataContainer;
import org.infinispan.test.AbstractInfinispanTest;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "conflict.impl.SegmentHasherBucketTest")
public class SegmentHasherBucketTest extends AbstractInfinispanTest {

   private InternalDataContainer<Object, Object> dataContainer;
   private Marshaller marshaller;
   private SegmentHasher hasher;

   @SuppressWarnings("unchecked")
   @BeforeMethod
   public void setUp() throws Exception {
      dataContainer = mock(InternalDataContainer.class);
      marshaller = mock(Marshaller.class);

      when(marshaller.objectToByteBuffer(any())).thenAnswer(invocation -> {
         Object arg = invocation.getArgument(0);
         return arg == null ? new byte[0] : arg.toString().getBytes();
      });

      hasher = new SegmentHasher(dataContainer, marshaller);
   }

   public void testEmptySegmentProducesZeroHashBuckets() {
      setupSegment(0);
      List<BucketHash> buckets = hasher.computeBucketHashes(0, 32);
      assertEquals(32, buckets.size());
      for (int i = 0; i < 32; i++) {
         assertEquals(0, buckets.get(i).segmentId());
         assertEquals(i, buckets.get(i).bucketId());
         assertEquals(0L, buckets.get(i).hash());
         assertEquals(0, buckets.get(i).entryCount());
      }
   }

   public void testSingleEntryLandsInCorrectBucket() {
      InternalCacheEntry<Object, Object> e1 = entry("key1", "value1");
      setupSegment(0, e1);

      List<BucketHash> buckets = hasher.computeBucketHashes(0, 32);
      int expectedBucket = hasher.bucketForKey("key1", 32);

      // Only the expected bucket should have an entry
      int nonEmptyCount = 0;
      for (int i = 0; i < 32; i++) {
         if (i == expectedBucket) {
            assertNotSame(0L, buckets.get(i).hash());
            assertEquals(1, buckets.get(i).entryCount());
         } else {
            if (buckets.get(i).entryCount() > 0) nonEmptyCount++;
         }
      }
      assertEquals("Only the target bucket should have entries", 0, nonEmptyCount);
   }

   public void testEntriesInDifferentBucketsGetIndependentHashes() {
      // Find two keys that map to different buckets
      String key1 = null, key2 = null;
      int bucket1 = -1;
      for (int i = 0; i < 1000; i++) {
         String candidate = "key" + i;
         int b = hasher.bucketForKey(candidate, 32);
         if (key1 == null) {
            key1 = candidate;
            bucket1 = b;
         } else if (b != bucket1) {
            key2 = candidate;
            break;
         }
      }
      assertTrue("Should find two keys in different buckets", key2 != null);

      // Compute with both entries
      setupSegment(0, entry(key1, "v1"), entry(key2, "v2"));
      List<BucketHash> buckets = hasher.computeBucketHashes(0, 32);

      int b1 = hasher.bucketForKey(key1, 32);
      int b2 = hasher.bucketForKey(key2, 32);
      assertTrue("Buckets should be different", b1 != b2);

      // Each bucket should have exactly one entry
      assertEquals(1, buckets.get(b1).entryCount());
      assertEquals(1, buckets.get(b2).entryCount());

      // Compute with only key1
      setupSegment(0, entry(key1, "v1"));
      List<BucketHash> bucketsKey1Only = hasher.computeBucketHashes(0, 32);

      // The bucket for key1 should have the same hash whether or not key2 is present
      assertEquals(bucketsKey1Only.get(b1).hash(), buckets.get(b1).hash());
   }

   public void testSameEntriesProduceSameBucketHashesRegardlessOfOrder() {
      InternalCacheEntry<Object, Object> e1 = entry("key1", "value1");
      InternalCacheEntry<Object, Object> e2 = entry("key2", "value2");
      InternalCacheEntry<Object, Object> e3 = entry("key3", "value3");

      // Order 1
      setupSegment(0, e1, e2, e3);
      List<BucketHash> order1 = hasher.computeBucketHashes(0, 32);

      // Order 2
      setupSegment(0, e3, e1, e2);
      List<BucketHash> order2 = hasher.computeBucketHashes(0, 32);

      // Order 3
      setupSegment(0, e2, e3, e1);
      List<BucketHash> order3 = hasher.computeBucketHashes(0, 32);

      for (int i = 0; i < 32; i++) {
         assertTrue("Bucket " + i + " should match across orderings",
               order1.get(i).matches(order2.get(i)));
         assertTrue("Bucket " + i + " should match across orderings",
               order2.get(i).matches(order3.get(i)));
      }
   }

   public void testBucketForKeyIsDeterministic() {
      // Same key should always map to the same bucket
      Set<Integer> results = new HashSet<>();
      for (int i = 0; i < 100; i++) {
         results.add(hasher.bucketForKey("testKey", 32));
      }
      assertEquals("bucketForKey should be deterministic", 1, results.size());
   }

   public void testBucketForKeyStaysInRange() {
      for (int i = 0; i < 200; i++) {
         int bucket = hasher.bucketForKey("key" + i, 32);
         assertTrue("Bucket should be >= 0", bucket >= 0);
         assertTrue("Bucket should be < 32", bucket < 32);
      }
   }

   public void testDeriveSegmentHashMatchesComputeHash() {
      InternalCacheEntry<Object, Object> e1 = entry("key1", "value1");
      InternalCacheEntry<Object, Object> e2 = entry("key2", "value2");
      InternalCacheEntry<Object, Object> e3 = entry("key3", "value3");

      // Compute segment hash directly (single-pass over entries)
      setupSegment(0, e1, e2, e3);
      SegmentHash directHash = hasher.computeHash(0);

      // Compute bucket hashes and derive segment hash from them
      setupSegment(0, e1, e2, e3);
      List<BucketHash> bucketHashes = hasher.computeBucketHashes(0, 32);
      SegmentHash derivedHash = SegmentHasher.deriveSegmentHash(0, bucketHashes);

      assertTrue("Derived segment hash should match direct computation",
            directHash.matches(derivedHash));
   }

   public void testDeriveSegmentHashEmptySegment() {
      setupSegment(0);
      SegmentHash directHash = hasher.computeHash(0);

      setupSegment(0);
      List<BucketHash> bucketHashes = hasher.computeBucketHashes(0, 32);
      SegmentHash derivedHash = SegmentHasher.deriveSegmentHash(0, bucketHashes);

      assertTrue(directHash.matches(derivedHash));
      assertEquals(0L, derivedHash.hash());
      assertEquals(0, derivedHash.entryCount());
   }

   public void testBucketHashMatches() {
      BucketHash h1 = new BucketHash(0, 5, 12345L, 10);
      BucketHash h2 = new BucketHash(0, 5, 12345L, 10);
      BucketHash h3 = new BucketHash(0, 5, 12345L, 11);
      BucketHash h4 = new BucketHash(0, 5, 99999L, 10);

      assertTrue(h1.matches(h2));
      assertTrue(!h1.matches(h3));
      assertTrue(!h1.matches(h4));
      assertTrue(!h1.matches(null));
   }

   @SuppressWarnings("unchecked")
   private void setupSegment(int segmentId, InternalCacheEntry<Object, Object>... entries) {
      Iterator<InternalCacheEntry<Object, Object>> iter = Arrays.asList(entries).iterator();
      when(dataContainer.iterator(IntSets.immutableSet(segmentId))).thenReturn(iter);
   }

   private InternalCacheEntry<Object, Object> entry(Object key, Object value) {
      return new ImmortalCacheEntry(key, value);
   }
}
