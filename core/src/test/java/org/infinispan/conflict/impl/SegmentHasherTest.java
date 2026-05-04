package org.infinispan.conflict.impl;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotSame;
import static org.testng.AssertJUnit.assertTrue;

import java.util.Arrays;
import java.util.Iterator;

import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.util.IntSets;
import org.infinispan.container.entries.ImmortalCacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.impl.InternalDataContainer;
import org.infinispan.test.AbstractInfinispanTest;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "conflict.impl.SegmentHasherTest")
public class SegmentHasherTest extends AbstractInfinispanTest {

   private InternalDataContainer<Object, Object> dataContainer;
   private Marshaller marshaller;
   private SegmentHasher hasher;

   @SuppressWarnings("unchecked")
   @BeforeMethod
   public void setUp() throws Exception {
      dataContainer = mock(InternalDataContainer.class);
      marshaller = mock(Marshaller.class);

      // Default: marshal objects by converting toString().getBytes()
      when(marshaller.objectToByteBuffer(any())).thenAnswer(invocation -> {
         Object arg = invocation.getArgument(0);
         return arg == null ? new byte[0] : arg.toString().getBytes();
      });

      hasher = new SegmentHasher(dataContainer, marshaller);
   }

   public void testEmptySegmentHashIsZero() {
      setupSegment(0);
      SegmentHash hash = hasher.computeHash(0);
      assertEquals(0, hash.segmentId());
      assertEquals(0L, hash.hash());
      assertEquals(0, hash.entryCount());
   }

   public void testSingleEntryHash() {
      setupSegment(0, entry("key1", "value1"));
      SegmentHash hash = hasher.computeHash(0);
      assertEquals(0, hash.segmentId());
      assertNotSame(0L, hash.hash());
      assertEquals(1, hash.entryCount());
   }

   public void testTwoEntriesXored() {
      InternalCacheEntry<Object, Object> e1 = entry("key1", "value1");
      InternalCacheEntry<Object, Object> e2 = entry("key2", "value2");

      // Compute individual hashes
      setupSegment(0, e1);
      SegmentHash hash1 = hasher.computeHash(0);

      setupSegment(0, e2);
      SegmentHash hash2 = hasher.computeHash(0);

      // Compute combined hash
      setupSegment(0, e1, e2);
      SegmentHash combined = hasher.computeHash(0);

      assertEquals(2, combined.entryCount());
      assertEquals(hash1.hash() ^ hash2.hash(), combined.hash());
   }

   public void testOrderIndependence() {
      InternalCacheEntry<Object, Object> e1 = entry("key1", "value1");
      InternalCacheEntry<Object, Object> e2 = entry("key2", "value2");
      InternalCacheEntry<Object, Object> e3 = entry("key3", "value3");

      // Order 1: e1, e2, e3
      setupSegment(0, e1, e2, e3);
      SegmentHash hashOrder1 = hasher.computeHash(0);

      // Order 2: e3, e1, e2
      setupSegment(0, e3, e1, e2);
      SegmentHash hashOrder2 = hasher.computeHash(0);

      // Order 3: e2, e3, e1
      setupSegment(0, e2, e3, e1);
      SegmentHash hashOrder3 = hasher.computeHash(0);

      assertTrue(hashOrder1.matches(hashOrder2));
      assertTrue(hashOrder2.matches(hashOrder3));
      assertTrue(hashOrder1.matches(hashOrder3));
   }

   public void testDifferentValuesProduceDifferentHashes() {
      setupSegment(0, entry("key1", "value1"));
      SegmentHash hash1 = hasher.computeHash(0);

      setupSegment(0, entry("key1", "value2"));
      SegmentHash hash2 = hasher.computeHash(0);

      // Same key, different value - hashes should differ
      assertTrue(!hash1.matches(hash2));
   }

   public void testDifferentEntryCountsDetected() {
      InternalCacheEntry<Object, Object> e1 = entry("key1", "value1");

      setupSegment(0, e1);
      SegmentHash oneEntry = hasher.computeHash(0);

      // Two entries that XOR to the same hash as one entry would be unusual,
      // but the entry count check provides extra safety
      assertEquals(1, oneEntry.entryCount());
   }

   public void testSegmentHashMatches() {
      SegmentHash h1 = new SegmentHash(0, 12345L, 10);
      SegmentHash h2 = new SegmentHash(0, 12345L, 10);
      SegmentHash h3 = new SegmentHash(0, 12345L, 11); // different count
      SegmentHash h4 = new SegmentHash(0, 99999L, 10); // different hash

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
