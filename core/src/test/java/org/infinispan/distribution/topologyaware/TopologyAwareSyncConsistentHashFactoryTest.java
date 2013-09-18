package org.infinispan.distribution.topologyaware;

import java.util.List;

import org.infinispan.distribution.ch.ConsistentHashFactory;
import org.infinispan.distribution.ch.DefaultConsistentHash;
import org.infinispan.distribution.ch.TopologyAwareSyncConsistentHashFactory;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.Test;

import static org.testng.Assert.assertTrue;

/**
 * @author Mircea.Markus@jboss.com
 * @author Dan Berindei
 * @since 5.2
 */
@Test(groups = "unit", testName = "distribution.topologyaware.TopologyAwareSyncConsistentHashFactoryTest")
public class TopologyAwareSyncConsistentHashFactoryTest extends TopologyAwareConsistentHashFactoryTest {

   private Log log = LogFactory.getLog(TopologyAwareSyncConsistentHashFactoryTest.class);

   public TopologyAwareSyncConsistentHashFactoryTest() {
      // Increase the number of segments to eliminate collisions (which would cause extra segment movements,
      // causing testConsistencyAfterLeave to fail.)
      numSegments = 1000;
   }

   @Override
   protected ConsistentHashFactory<DefaultConsistentHash> createConsistentHashFactory() {
      return new TopologyAwareSyncConsistentHashFactory();
   }

   @Override
   protected void assertDistribution(int numOwners, List<Address> currentMembers) {
      TopologyAwareOwnershipStatistics stats = new TopologyAwareOwnershipStatistics(ch);
      log.tracef("Ownership stats: " + stats);
      int maxPrimarySegments = numSegments / currentMembers.size() + 1;
      for (Address node : currentMembers) {
         int maxSegments = stats.computeExpectedSegments(numSegments, numOwners, node);
         log.tracef("Primary segments ratio: %f, total segments ratio: %f",
               stats.getPrimaryOwned(node) / maxPrimarySegments, stats.getOwned(node) / maxSegments);
         assertTrue(maxPrimarySegments * 0.4 <= stats.getPrimaryOwned(node));
         assertTrue(stats.getPrimaryOwned(node) <= maxPrimarySegments * 2);
         assertTrue(maxSegments * 0.4 <= stats.getOwned(node));
         assertTrue(stats.getOwned(node) <= maxSegments * 2);
      }
   }
}
