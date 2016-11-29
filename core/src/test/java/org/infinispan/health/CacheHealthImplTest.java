package org.infinispan.health;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.testng.Assert.assertEquals;

import org.infinispan.cache.impl.CacheImpl;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.health.impl.CacheHealthImpl;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.partitionhandling.AvailabilityMode;
import org.testng.annotations.Test;

@Test(testName = "health.CacheHealthImplTest", groups = "functional")
public class CacheHealthImplTest {

    @Test
    public void testHealthyStatus() throws Exception {
        //given
        CacheImpl<Object, Object> cache = spy(new CacheImpl<>("test"));
        DistributionManager distributionManagerMock = mock(DistributionManager.class);

        doReturn(false).when(distributionManagerMock).isRehashInProgress();
        doReturn(distributionManagerMock).when(cache).getDistributionManager();
        doReturn(ComponentStatus.RUNNING).when(cache).getStatus();
        doReturn(AvailabilityMode.AVAILABLE).when(cache).getAvailability();

        CacheHealth cacheHealth = new CacheHealthImpl(cache);

        //when
        HealthStatus status = cacheHealth.getStatus();

        //then
        assertEquals(status, HealthStatus.HEALTHY);
    }

    @Test
    public void testUnhealthyStatusWithFailedComponent() throws Exception {
        //given
        CacheImpl<Object, Object> cache = spy(new CacheImpl<>("test"));

        doReturn(ComponentStatus.FAILED).when(cache).getStatus();

        CacheHealth cacheHealth = new CacheHealthImpl(cache);

        //when
        HealthStatus status = cacheHealth.getStatus();

        //then
        assertEquals(status, HealthStatus.UNHEALTHY);
    }

    @Test
    public void testUnhealthyStatusWithTerminatedComponent() throws Exception {
        //given
        CacheImpl<Object, Object> cache = spy(new CacheImpl<>("test"));

        doReturn(ComponentStatus.TERMINATED).when(cache).getStatus();

        CacheHealth cacheHealth = new CacheHealthImpl(cache);

        //when
        HealthStatus status = cacheHealth.getStatus();

        //then
        assertEquals(status, HealthStatus.UNHEALTHY);
    }

    @Test
    public void testUnhealthyStatusWithStoppingComponent() throws Exception {
        //given
        CacheImpl<Object, Object> cache = spy(new CacheImpl<>("test"));

        doReturn(ComponentStatus.STOPPING).when(cache).getStatus();

        CacheHealth cacheHealth = new CacheHealthImpl(cache);

        //when
        HealthStatus status = cacheHealth.getStatus();

        //then
        assertEquals(status, HealthStatus.UNHEALTHY);
    }

    @Test
    public void testUnhealthyStatusWithDegradedPartition() throws Exception {
        //given
        CacheImpl<Object, Object> cache = spy(new CacheImpl<>("test"));

        doReturn(ComponentStatus.RUNNING).when(cache).getStatus();
        doReturn(AvailabilityMode.DEGRADED_MODE).when(cache).getAvailability();

        CacheHealth cacheHealth = new CacheHealthImpl(cache);

        //when
        HealthStatus status = cacheHealth.getStatus();

        //then
        assertEquals(status, HealthStatus.UNHEALTHY);
    }

    @Test
    public void testRebalancingStatusOnRebalance() throws Exception {
        //given
        CacheImpl<Object, Object> cache = spy(new CacheImpl<>("test"));
        DistributionManager distributionManagerMock = mock(DistributionManager.class);

        doReturn(true).when(distributionManagerMock).isRehashInProgress();
        doReturn(distributionManagerMock).when(cache).getDistributionManager();
        doReturn(ComponentStatus.RUNNING).when(cache).getStatus();
        doReturn(AvailabilityMode.AVAILABLE).when(cache).getAvailability();

        CacheHealth cacheHealth = new CacheHealthImpl(cache);

        //when
        HealthStatus status = cacheHealth.getStatus();

        //then
        assertEquals(status, HealthStatus.REBALANCING);
    }

    @Test
    public void testReturningName() throws Exception {
        //given
        CacheImpl<Object, Object> cache = new CacheImpl<>("test");

        //when
        String name = cache.getName();

        //then
        assertEquals(name, "test");
    }
}
