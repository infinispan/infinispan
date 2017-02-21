package org.infinispan.partitionhandling;

import java.util.Collections;
import java.util.Optional;

import org.infinispan.partitionhandling.impl.AvailabilityStrategyContext;
import org.infinispan.partitionhandling.impl.PreferConsistencyStrategy;
import org.infinispan.topology.ClusterCacheStatus;
import org.infinispan.topology.ClusterTopologyManager;
import org.infinispan.topology.ClusterTopologyManagerImpl;
import org.infinispan.topology.PersistentUUIDManager;
import org.infinispan.topology.PersistentUUIDManagerImpl;
import org.infinispan.topology.RebalancingStatus;
import org.infinispan.util.logging.events.EventLogManager;
import org.infinispan.util.logging.events.impl.EventLogManagerImpl;
import org.mockito.Mockito;
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

      preferConsistencyStrategy = new PreferConsistencyStrategy(eventLogManager, persistentUUIDManager);
      status = new ClusterCacheStatus("does-not-matter", preferConsistencyStrategy, topologyManager, null, Optional.empty(), persistentUUIDManager);
   }

   public void testAvoidingNullPointerExceptionWhenUpdatingPartitionWithNullTopology() {
      //given
      AvailabilityStrategyContext availabilityStrategyContext = Mockito.mock(AvailabilityStrategyContext.class);

      //when
      preferConsistencyStrategy.onPartitionMerge(availabilityStrategyContext, Collections.emptyList());

      //then
      Assert.assertNull(status.getCurrentTopology());
      Assert.assertNull(status.getStableTopology());
      Assert.assertEquals(AvailabilityMode.AVAILABLE, status.getAvailabilityMode());
      Assert.assertEquals(RebalancingStatus.COMPLETE, status.getRebalancingStatus());
   }


}
