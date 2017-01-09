package org.infinispan.distribution;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;

import java.util.HashMap;
import java.util.Map;

import org.infinispan.statetransfer.OutdatedTopologyException;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.Exceptions;
import org.infinispan.topology.CacheTopology;
import org.mockito.Mockito;
import org.testng.annotations.Test;

/**
 * Unit test for {@link TriangleOrderManager}.
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
@Test(groups = "unit", testName = "distribution.TriangleOrderManagerTest")
public class TriangleOrderManagerTest extends AbstractInfinispanTest {

   private static CacheTopology mockCacheTopology(int topologyId) {
      CacheTopology mock = Mockito.mock(CacheTopology.class);
      Mockito.when(mock.getTopologyId()).thenReturn(topologyId);
      return mock;
   }

   private static Map<Integer, Long> newSegmentMap() {
      Map<Integer, Long> map = new HashMap<>();
      map.put(1, -1L);
      map.put(2, -1L);
      map.put(3, -1L);
      return map;
   }

   private static Map<Integer, Long> newSegmentMap(long sequence1, long sequence2, long sequence3) {
      Map<Integer, Long> map = new HashMap<>();
      map.put(1, sequence1);
      map.put(2, sequence2);
      map.put(3, sequence3);
      return map;
   }

   private static void assertSequenceMap(Map<Integer, Long> map, long sequence1, long sequence2, long sequence3) {
      assertEquals(sequence1, (long) map.get(1));
      assertEquals(sequence2, (long) map.get(2));
      assertEquals(sequence3, (long) map.get(3));
   }

   public void testInvalidTopologyId() {
      TriangleOrderManager triangleOrderManager = new TriangleOrderManager(4);
      triangleOrderManager.updateCacheTopology(mockCacheTopology(1));

      try {
         triangleOrderManager.next(0, 0);
         fail("Exception expected!");
      } catch (OutdatedTopologyException e) {
         Exceptions.assertException(OutdatedTopologyException.class, e);
      }

      try {
         triangleOrderManager.next(1, 2);
         fail("Exception expected!");
      } catch (OutdatedTopologyException e) {
         Exceptions.assertException(OutdatedTopologyException.class, e);
      }

      try {
         triangleOrderManager.next(newSegmentMap(), 0);
         fail("Exception expected!");
      } catch (OutdatedTopologyException e) {
         Exceptions.assertException(OutdatedTopologyException.class, e);
      }

      try {
         triangleOrderManager.next(newSegmentMap(), 2);
         fail("Exception expected!");
      } catch (OutdatedTopologyException e) {
         Exceptions.assertException(OutdatedTopologyException.class, e);
      }
   }

   public void testSequence() {
      TriangleOrderManager triangleOrderManager = new TriangleOrderManager(4);
      triangleOrderManager.updateCacheTopology(mockCacheTopology(0));

      assertEquals(1, triangleOrderManager.next(0, 0));
      assertEquals(1, triangleOrderManager.next(1, 0));
      assertEquals(2, triangleOrderManager.next(1, 0));
      Map<Integer, Long> map = newSegmentMap();
      triangleOrderManager.next(map, 0);
      assertSequenceMap(map, 3, 1, 1);
   }

   public void testSequenceWithTopologyChange() {
      int topologyId = 1;
      TriangleOrderManager triangleOrderManager = new TriangleOrderManager(5);
      triangleOrderManager.updateCacheTopology(mockCacheTopology(topologyId));

      assertEquals(1, triangleOrderManager.next(1, topologyId));
      assertEquals(2, triangleOrderManager.next(1, topologyId));
      Map<Integer, Long> map = newSegmentMap();
      triangleOrderManager.next(map, topologyId);
      assertSequenceMap(map, 3, 1, 1);

      triangleOrderManager.updateCacheTopology(mockCacheTopology(++topologyId));
      map = newSegmentMap();
      triangleOrderManager.next(map, topologyId);
      assertSequenceMap(map, 1, 1, 1);
      assertEquals(2, triangleOrderManager.next(1, topologyId));
      assertEquals(3, triangleOrderManager.next(1, topologyId));
      assertEquals(1, triangleOrderManager.next(4, topologyId));

      triangleOrderManager.updateCacheTopology(mockCacheTopology(++topologyId));
      assertEquals(1, triangleOrderManager.next(1, topologyId));
      assertEquals(1, triangleOrderManager.next(2, topologyId));
      assertEquals(1, triangleOrderManager.next(3, topologyId));
      assertEquals(1, triangleOrderManager.next(4, topologyId));
      map = newSegmentMap();
      triangleOrderManager.next(map, topologyId);
      assertSequenceMap(map, 2, 2, 2);
   }

   public void testDeliverOrder() {
      TriangleOrderManager triangleOrderManager = new TriangleOrderManager(4);
      triangleOrderManager.updateCacheTopology(mockCacheTopology(0));

      assertTrue(triangleOrderManager.isNext(1, 1, 0));
      assertFalse(triangleOrderManager.isNext(1, 2, 0));
      assertFalse(triangleOrderManager.isNext(1, 3, 0));

      triangleOrderManager.markDelivered(1, 1, 0);
      assertTrue(triangleOrderManager.isNext(1, 2, 0));
      assertFalse(triangleOrderManager.isNext(1, 3, 0));

      triangleOrderManager.markDelivered(1, 2, 0);
      assertTrue(triangleOrderManager.isNext(1, 3, 0));

      triangleOrderManager.markDelivered(1, 3, 0);
      Map<Integer, Long> map = newSegmentMap(4, 2, 3);
      assertFalse(triangleOrderManager.isNext(map, 0));

      triangleOrderManager.markDelivered(2, 1, 0);
      assertFalse(triangleOrderManager.isNext(map, 0));

      triangleOrderManager.markDelivered(3, 1, 0);
      assertFalse(triangleOrderManager.isNext(map, 0));

      triangleOrderManager.markDelivered(3, 2, 0);
      assertTrue(triangleOrderManager.isNext(map, 0));
   }

   public void testUnblockOldTopology() {
      TriangleOrderManager triangleOrderManager = new TriangleOrderManager(4);
      triangleOrderManager.updateCacheTopology(mockCacheTopology(1));

      //same topology, but incorrect sequence number
      Map<Integer, Long> map = newSegmentMap(2, 3, 4);
      assertFalse(triangleOrderManager.isNext(map, 1));
      assertFalse(triangleOrderManager.isNext(0, 2, 1));

      //lower topology. should unlock everything
      triangleOrderManager.updateCacheTopology(mockCacheTopology(2));
      assertTrue(triangleOrderManager.isNext(map, 1));
      assertTrue(triangleOrderManager.isNext(0, 2, 1));

      //higher topology than current one, everything is blocked
      map = newSegmentMap(1, 1, 1);
      assertFalse(triangleOrderManager.isNext(map, 3));
      assertFalse(triangleOrderManager.isNext(0, 1, 3));

      //unlocks everything (correct sequence number)
      triangleOrderManager.updateCacheTopology(mockCacheTopology(3));
      assertTrue(triangleOrderManager.isNext(map, 3));
      assertTrue(triangleOrderManager.isNext(0, 1, 3));
   }

}
