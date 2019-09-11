package org.infinispan.health.impl;

import static org.testng.Assert.assertTrue;

import org.infinispan.health.HostInfo;
import org.testng.annotations.Test;

@Test(testName = "health.impl.HostInfoImplTest", groups = "functional")
public class HostInfoImplTest {

    @Test
    public void testReturningValuesFromHostInfo() {
        //given
        HostInfo hostInfo = new HostInfoImpl();

        //when
        int numberOfCpus = hostInfo.getNumberOfCpus();
        long freeMemoryInKb = hostInfo.getFreeMemoryInKb();
        long totalMemoryKb = hostInfo.getTotalMemoryKb();

        //then
        assertTrue(numberOfCpus > 0);
        assertTrue(freeMemoryInKb > 0);
        assertTrue(totalMemoryKb > 0);
    }
}
