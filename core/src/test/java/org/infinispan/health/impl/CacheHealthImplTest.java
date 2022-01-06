package org.infinispan.health.impl;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;

import org.infinispan.distribution.DistributionManager;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.health.CacheHealth;
import org.infinispan.health.HealthStatus;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.partitionhandling.AvailabilityMode;
import org.infinispan.partitionhandling.impl.PartitionHandlingManager;
import org.testng.annotations.Test;

@Test(testName = "health.impl.CacheHealthImplTest", groups = "functional")
public class CacheHealthImplTest {

    @Test
    public void testHealthyStatus() {
        //given
        ComponentRegistry componentRegistryMock = mock(ComponentRegistry.class);

        DistributionManager distributionManagerMock = mock(DistributionManager.class);
        doReturn(false).when(distributionManagerMock).isRehashInProgress();
        doReturn(distributionManagerMock).when(componentRegistryMock).getDistributionManager();

        doReturn(ComponentStatus.RUNNING).when(componentRegistryMock).getStatus();

        PartitionHandlingManager partitionHandlingManagerMock = mock(PartitionHandlingManager.class);
        doReturn(AvailabilityMode.AVAILABLE).when(partitionHandlingManagerMock).getAvailabilityMode();
        doReturn(partitionHandlingManagerMock).when(componentRegistryMock).getComponent(eq(PartitionHandlingManager.class));

        CacheHealth cacheHealth = new CacheHealthImpl(componentRegistryMock);

        //when
        HealthStatus status = cacheHealth.getStatus();

        //then
        assertEquals(status, HealthStatus.HEALTHY);
    }

    @Test
    public void testUnhealthyStatusWithFailedComponent() {
        //given
        ComponentRegistry componentRegistryMock = mock(ComponentRegistry.class);

        doReturn(ComponentStatus.FAILED).when(componentRegistryMock).getStatus();

        CacheHealth cacheHealth = new CacheHealthImpl(componentRegistryMock);

        //when
        HealthStatus status = cacheHealth.getStatus();

        //then
        assertEquals(status, HealthStatus.DEGRADED);
    }

    @Test
    public void testUnhealthyStatusWithTerminatedComponent() {
        //given
        ComponentRegistry componentRegistryMock = mock(ComponentRegistry.class);

        doReturn(ComponentStatus.TERMINATED).when(componentRegistryMock).getStatus();

        CacheHealth cacheHealth = new CacheHealthImpl(componentRegistryMock);

        //when
        HealthStatus status = cacheHealth.getStatus();

        //then
        assertEquals(status, HealthStatus.DEGRADED);
    }

    @Test
    public void testUnhealthyStatusWithStoppingComponent() {
        //given
        ComponentRegistry componentRegistryMock = mock(ComponentRegistry.class);

        doReturn(ComponentStatus.STOPPING).when(componentRegistryMock).getStatus();

        CacheHealth cacheHealth = new CacheHealthImpl(componentRegistryMock);

        //when
        HealthStatus status = cacheHealth.getStatus();

        //then
        assertEquals(status, HealthStatus.DEGRADED);
    }

    @Test
    public void testUnhealthyStatusWithDegradedPartition() {
        //given
        ComponentRegistry componentRegistryMock = mock(ComponentRegistry.class);

        doReturn(ComponentStatus.RUNNING).when(componentRegistryMock).getStatus();

        PartitionHandlingManager partitionHandlingManagerMock = mock(PartitionHandlingManager.class);
        doReturn(AvailabilityMode.DEGRADED_MODE).when(partitionHandlingManagerMock).getAvailabilityMode();
        doReturn(partitionHandlingManagerMock).when(componentRegistryMock).getComponent(eq(PartitionHandlingManager.class));

        CacheHealth cacheHealth = new CacheHealthImpl(componentRegistryMock);

        //when
        HealthStatus status = cacheHealth.getStatus();

        //then
        assertEquals(status, HealthStatus.DEGRADED);
    }

    @Test
    public void testRebalancingStatusOnRebalance() {
        //given
        ComponentRegistry componentRegistryMock = mock(ComponentRegistry.class);
        DistributionManager distributionManagerMock = mock(DistributionManager.class);

        doReturn(true).when(distributionManagerMock).isRehashInProgress();
        doReturn(distributionManagerMock).when(componentRegistryMock).getDistributionManager();
        doReturn(ComponentStatus.RUNNING).when(componentRegistryMock).getStatus();

        PartitionHandlingManager partitionHandlingManagerMock = mock(PartitionHandlingManager.class);
        doReturn(AvailabilityMode.AVAILABLE).when(partitionHandlingManagerMock).getAvailabilityMode();
        doReturn(partitionHandlingManagerMock).when(componentRegistryMock).getComponent(eq(PartitionHandlingManager.class));

        CacheHealth cacheHealth = new CacheHealthImpl(componentRegistryMock);

        //when
        HealthStatus status = cacheHealth.getStatus();

        //then
        assertEquals(status, HealthStatus.HEALTHY_REBALANCING);
    }

    @Test
    public void testReturningName() {
        //given
        ComponentRegistry componentRegistryMock = mock(ComponentRegistry.class);
        doReturn("test").when(componentRegistryMock).getCacheName();

        CacheHealth cacheHealth = new CacheHealthImpl(componentRegistryMock);

        //when
        String name = cacheHealth.getCacheName();

        //then
        assertEquals(name, "test");
    }
}
