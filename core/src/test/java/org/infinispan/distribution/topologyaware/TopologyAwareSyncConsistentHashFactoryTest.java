package org.infinispan.distribution.topologyaware;

import static org.testng.Assert.assertTrue;

import java.util.List;

import org.infinispan.distribution.ch.impl.ConsistentHashFactory;
import org.infinispan.distribution.ch.impl.DefaultConsistentHash;
import org.infinispan.distribution.ch.impl.TopologyAwareSyncConsistentHashFactory;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.Test;

/**
 * @author Mircea.Markus@jboss.com
 * @author Dan Berindei
 * @since 5.2
 */
@Test(groups = "unit", testName = "distribution.topologyaware.TopologyAwareSyncConsistentHashFactoryTest")
public class TopologyAwareSyncConsistentHashFactoryTest extends TopologyAwareConsistentHashFactoryTest {

   private final Log log = LogFactory.getLog(TopologyAwareSyncConsistentHashFactoryTest.class);

   @Override
   protected ConsistentHashFactory<DefaultConsistentHash> createConsistentHashFactory() {
      return TopologyAwareSyncConsistentHashFactory.getInstance();
   }

   @Override
   protected void assertDistribution(List<Address> currentMembers, int numOwners, int numSegments) {
      TopologyAwareOwnershipStatistics stats = new TopologyAwareOwnershipStatistics(ch);
      log.tracef("Ownership stats: " + stats);
      for (Address node : currentMembers) {
         float expectedPrimarySegments = stats.computeExpectedPrimarySegments(node);
         float expectedOwnedSegments = stats.computeExpectedOwnedSegments(node);
         int primaryOwned = stats.getPrimaryOwned(node);
         int owned = stats.getOwned(node);
         assertTrue(Math.floor(0.7 * expectedPrimarySegments) <= primaryOwned);
         assertTrue(primaryOwned <= Math.ceil(1.2 * expectedPrimarySegments));
         assertTrue(Math.floor(0.7 * expectedOwnedSegments) <= owned);
         assertTrue(owned <= Math.ceil(1.2 * expectedOwnedSegments));
      }
   }
}
