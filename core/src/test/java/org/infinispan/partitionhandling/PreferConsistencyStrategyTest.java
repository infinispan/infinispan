package org.infinispan.partitionhandling;

import static org.mockito.Mockito.mock;

import java.util.Collections;
import java.util.Optional;

import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.partitionhandling.impl.PreferConsistencyStrategy;
import org.infinispan.statetransfer.RebalanceType;
import org.infinispan.topology.ClusterCacheStatus;
import org.infinispan.topology.ClusterTopologyManager;
import org.infinispan.topology.ClusterTopologyManagerImpl;
import org.infinispan.topology.PersistentUUIDManager;
import org.infinispan.topology.PersistentUUIDManagerImpl;
import org.infinispan.topology.RebalancingStatus;
import org.infinispan.util.logging.events.EventLogManager;
import org.infinispan.util.logging.events.impl.EventLogManagerImpl;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "partitionhandling.PreferConsistencyStrategyTest")
public class PreferConsistencyStrategyTest {

   private PreferConsistencyStrategy preferConsistencyStrategy;
   private ClusterCacheStatus status;

   @BeforeMethod
   public void beforeMethod() {
      EventLogManager eventLogManager = new EventLogManagerImpl();
      PersistentUUIDManager persistentUUIDManager = new PersistentUUIDManagerImpl();
      ClusterTopologyManager topologyManager = new ClusterTopologyManagerImpl();
      EmbeddedCacheManager cacheManager = mock(EmbeddedCacheManager.class);

      preferConsistencyStrategy = new PreferConsistencyStrategy(eventLogManager, persistentUUIDManager, null);
      status = new ClusterCacheStatus(cacheManager, "does-not-matter", preferConsistencyStrategy, RebalanceType.FOUR_PHASE, topologyManager,
                                      null, persistentUUIDManager, eventLogManager, Optional.empty(), false);
   }

   public void testAvoidingNullPointerExceptionWhenUpdatingPartitionWithNullTopology() {
      //when
      preferConsistencyStrategy.onPartitionMerge(status, Collections.emptyMap());

      //then
      Assert.assertNull(status.getCurrentTopology());
      Assert.assertNull(status.getStableTopology());
      Assert.assertEquals(AvailabilityMode.AVAILABLE, status.getAvailabilityMode());
      Assert.assertEquals(RebalancingStatus.COMPLETE, status.getRebalancingStatus());
   }


}
