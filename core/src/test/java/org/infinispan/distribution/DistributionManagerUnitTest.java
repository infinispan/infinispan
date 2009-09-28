package org.infinispan.distribution;

import org.easymock.EasyMock;
import org.infinispan.remoting.transport.Address;
import org.testng.annotations.Test;

import java.util.Arrays;

/**
 * Tests helper functions on the DistManager
 *
 * @author Manik Surtani
 * @since 4.0
 */
@Test(groups = "unit", testName = "distribution.DistributionManagerUnitTest", enabled = false)
public class DistributionManagerUnitTest {
   DistributionManagerImpl dmi = new DistributionManagerImpl();

   public void testDeterminingLeaversAndJoiners() {
      Address a1 = EasyMock.createNiceMock(Address.class);
      Address a2 = EasyMock.createNiceMock(Address.class);
      Address a3 = EasyMock.createNiceMock(Address.class);
      Address a4 = EasyMock.createNiceMock(Address.class);
      Address a5 = EasyMock.createNiceMock(Address.class);

      Address newAddress = dmi.diff(Arrays.asList(a1, a2, a3, a4),
                                    Arrays.asList(a1, a2, a3, a4, a5));

      assert newAddress == a5 : "Expecting " + a5 + " but was " + newAddress;

      newAddress = dmi.diff(Arrays.asList(a1, a2, a3, a4),
                            Arrays.asList(a5, a4, a3, a2, a1));

      assert newAddress == a5 : "Expecting " + a5 + " but was " + newAddress;

      newAddress = dmi.diff(Arrays.asList(a1, a2, a3, a4, a5),
                            Arrays.asList(a1, a2, a3, a4));

      assert newAddress == a5 : "Expecting " + a5 + " but was " + newAddress;

      newAddress = dmi.diff(Arrays.asList(a5, a4, a3, a2, a1),
                            Arrays.asList(a1, a2, a3, a4));

      assert newAddress == a5 : "Expecting " + a5 + " but was " + newAddress;
   }
}
