package org.infinispan.health;

import static org.testng.Assert.assertTrue;

import org.infinispan.health.impl.HostInfoImpl;
import org.testng.annotations.Test;

@Test(testName = "health.HostInfoImplTest", groups = "functional")
public class HostInfoImplTest {

    @Test
    public void testReturningValuesFromHostInfo() throws Exception {
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
