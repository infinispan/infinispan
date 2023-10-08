package org.infinispan.distribution;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;

import java.util.Collections;
import java.util.List;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.distribution.ch.impl.ReplicatedConsistentHash;
import org.infinispan.remoting.transport.Address;
import org.infinispan.statetransfer.OutdatedTopologyException;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.commons.test.Exceptions;
import org.infinispan.test.TestingUtil;
import org.infinispan.topology.CacheTopology;
import org.testng.annotations.Test;

/**
 * Unit test for {@link TriangleOrderManager}.
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
@Test(groups = "unit", testName = "distribution.TriangleOrderManagerTest")
public class TriangleOrderManagerTest extends AbstractInfinispanTest {

   private static final Address LOCAL_ADDRESS = new TestAddress(0, "A");

   private static LocalizedCacheTopology mockCacheTopology(int topologyId) {
      List<Address> members = Collections.singletonList(LOCAL_ADDRESS);
      ConsistentHash ch = new ReplicatedConsistentHash(members, List.of(0));
      CacheTopology cacheTopology = new CacheTopology(topologyId, 0, ch, null, CacheTopology.Phase.NO_REBALANCE, members, null);
      return new LocalizedCacheTopology(CacheMode.DIST_SYNC, cacheTopology, key -> 0, LOCAL_ADDRESS, true);
   }

   public void testInvalidTopologyId() {
      TriangleOrderManager triangleOrderManager = new TriangleOrderManager(4);
      DistributionManager mockDistributionManager = mock(DistributionManager.class);
      when(mockDistributionManager.getCacheTopology()).thenReturn(mockCacheTopology(1));
      TestingUtil.inject(triangleOrderManager, mockDistributionManager);

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
   }

   public void testSequence() {
      TriangleOrderManager triangleOrderManager = new TriangleOrderManager(4);
      DistributionManager mockDistributionManager = mock(DistributionManager.class);
      when(mockDistributionManager.getCacheTopology()).thenReturn(mockCacheTopology(0));
      TestingUtil.inject(triangleOrderManager, mockDistributionManager);

      assertEquals(1, triangleOrderManager.next(0, 0));
      assertEquals(1, triangleOrderManager.next(1, 0));
      assertEquals(2, triangleOrderManager.next(1, 0));
   }

   public void testSequenceWithTopologyChange() {
      int topologyId = 1;
      TriangleOrderManager triangleOrderManager = new TriangleOrderManager(5);
      DistributionManager mockDistributionManager = mock(DistributionManager.class);
      when(mockDistributionManager.getCacheTopology()).thenReturn(mockCacheTopology(topologyId));
      TestingUtil.inject(triangleOrderManager, mockDistributionManager);

      assertEquals(1, triangleOrderManager.next(1, topologyId));
      assertEquals(2, triangleOrderManager.next(1, topologyId));

      when(mockDistributionManager.getCacheTopology()).thenReturn(mockCacheTopology(++topologyId));
      assertEquals(1, triangleOrderManager.next(1, topologyId));
      assertEquals(2, triangleOrderManager.next(1, topologyId));
      assertEquals(1, triangleOrderManager.next(4, topologyId));

      when(mockDistributionManager.getCacheTopology()).thenReturn(mockCacheTopology(++topologyId));
      assertEquals(1, triangleOrderManager.next(1, topologyId));
      assertEquals(1, triangleOrderManager.next(2, topologyId));
      assertEquals(1, triangleOrderManager.next(3, topologyId));
      assertEquals(1, triangleOrderManager.next(4, topologyId));
   }

   public void testDeliverOrder() {
      TriangleOrderManager triangleOrderManager = new TriangleOrderManager(4);
      DistributionManager mockDistributionManager = mock(DistributionManager.class);
      when(mockDistributionManager.getCacheTopology()).then(i -> mockCacheTopology(0));
      TestingUtil.inject(triangleOrderManager, mockDistributionManager);

      assertTrue(triangleOrderManager.isNext(1, 1, 0));
      assertFalse(triangleOrderManager.isNext(1, 2, 0));
      assertFalse(triangleOrderManager.isNext(1, 3, 0));

      triangleOrderManager.markDelivered(1, 1, 0);
      assertTrue(triangleOrderManager.isNext(1, 2, 0));
      assertFalse(triangleOrderManager.isNext(1, 3, 0));

      triangleOrderManager.markDelivered(1, 2, 0);
      assertTrue(triangleOrderManager.isNext(1, 3, 0));

      triangleOrderManager.markDelivered(1, 3, 0);

      triangleOrderManager.markDelivered(2, 1, 0);

      triangleOrderManager.markDelivered(3, 1, 0);

      triangleOrderManager.markDelivered(3, 2, 0);
   }

   public void testUnblockOldTopology() {
      TriangleOrderManager triangleOrderManager = new TriangleOrderManager(4);
      DistributionManager mockDistributionManager = mock(DistributionManager.class);
      when(mockDistributionManager.getCacheTopology()).thenReturn(mockCacheTopology(1));
      TestingUtil.inject(triangleOrderManager, mockDistributionManager);

      //same topology, but incorrect sequence number
      assertFalse(triangleOrderManager.isNext(0, 2, 1));

      //lower topology. should unlock everything
      when(mockDistributionManager.getCacheTopology()).thenReturn(mockCacheTopology(2));
      assertTrue(triangleOrderManager.isNext(0, 2, 1));

      //higher topology than current one, everything is blocked
      assertFalse(triangleOrderManager.isNext(0, 1, 3));

      //unlocks everything (correct sequence number)
      when(mockDistributionManager.getCacheTopology()).thenReturn(mockCacheTopology(3));
      assertTrue(triangleOrderManager.isNext(0, 1, 3));
   }

}
