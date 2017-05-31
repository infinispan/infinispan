package org.infinispan.partitionhandling;


import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;

import org.infinispan.commons.hash.MurmurHash3;
import org.infinispan.distribution.TestAddress;
import org.infinispan.distribution.ch.impl.DefaultConsistentHashFactory;
import org.infinispan.partitionhandling.impl.PreferConsistencyStrategy;
import org.infinispan.remoting.transport.Address;
import org.infinispan.topology.CacheJoinInfo;
import org.infinispan.topology.CacheStatusResponse;
import org.infinispan.topology.CacheTopology;
import org.infinispan.topology.ClusterCacheStatus;
import org.infinispan.topology.ClusterTopologyManager;
import org.infinispan.topology.PersistentUUID;
import org.infinispan.topology.PersistentUUIDManager;
import org.infinispan.topology.PersistentUUIDManagerImpl;
import org.infinispan.util.logging.events.EventLogManager;
import org.infinispan.util.logging.events.impl.EventLogManagerImpl;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "partitionhandling.PreferConsistencyStrategyTest")
public class PreferConsistencyStrategyTest {

   private PreferConsistencyStrategy preferConsistencyStrategy;
   private PersistentUUIDManager manager;
   private ClusterTopologyManager topologyManager;
   private EventLogManager eventLogManager;

   @BeforeMethod
   public void beforeMethod() {
      manager = new PersistentUUIDManagerImpl();
      eventLogManager = new EventLogManagerImpl();
      topologyManager = Mockito.mock(ClusterTopologyManager.class);

      preferConsistencyStrategy = new PreferConsistencyStrategy(eventLogManager, manager);
   }

   public void testAvoidingNullPointerExceptionWhenUpdatingPartitionWithNullTopology() {
      //given
      ClusterCacheStatus status = new ClusterCacheStatus("does-not-matter", preferConsistencyStrategy, topologyManager, null, Optional.empty(), manager);

      //when
      preferConsistencyStrategy.onPartitionMerge(status, Collections.emptyList());

      //then
      Assert.assertNull(status.getCurrentTopology());
      Assert.assertNull(status.getStableTopology());
      Assert.assertEquals(AvailabilityMode.AVAILABLE, status.getAvailabilityMode());
   }

   public void testCoordinatorHasNewerTopology() throws Exception {
      Address myAddress = new TestAddress(1);
      Address otherAddress = new TestAddress(2);

      manager.addPersistentAddressMapping(myAddress, PersistentUUID.randomUUID());
      manager.addPersistentAddressMapping(otherAddress, PersistentUUID.randomUUID());

      ClusterCacheStatus coordinatorStatus = new ClusterCacheStatus("does-not-matter", preferConsistencyStrategy, topologyManager, null, Optional.empty(), manager);
      coordinatorStatus.doJoin(myAddress, createJoinInfo(myAddress));
      coordinatorStatus.doJoin(otherAddress, createJoinInfo(otherAddress));
      // at this stage current topology ID from CacheStatus is 2

      // The cache response contains non-null stable topology but null current topology
      CacheStatusResponse responseFromOtherNode = new CacheStatusResponse(null,
            null,
            createCacheTopology(1, myAddress, otherAddress),
            AvailabilityMode.AVAILABLE);

      //when
      preferConsistencyStrategy.onPartitionMerge(coordinatorStatus, Arrays.asList(responseFromOtherNode));

      //then
      Assert.assertNull(coordinatorStatus.getCurrentTopology());
      Assert.assertNull(coordinatorStatus.getStableTopology());
      Assert.assertEquals(AvailabilityMode.AVAILABLE, coordinatorStatus.getAvailabilityMode());
      Assert.assertEquals(2, coordinatorStatus.getExpectedMembers().size());

   }

   private CacheTopology createCacheTopology(int topologyId, Address... address) {
      return new CacheTopology(topologyId, 0, null, null, CacheTopology.Phase.NO_REBALANCE, Arrays.asList(address), Collections.emptyList());
   }

   private CacheJoinInfo createJoinInfo(Address address) {
      return new CacheJoinInfo(new DefaultConsistentHashFactory(),
            MurmurHash3.getInstance(),
            10,
            2,
            100,
            false,
            true,
            1.0f,
            manager.getPersistentUuid(address),
            Optional.empty());
   }
}
