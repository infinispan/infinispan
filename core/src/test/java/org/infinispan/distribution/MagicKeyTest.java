package org.infinispan.distribution;

import org.testng.annotations.Test;


@Test(groups = "unit", testName = "distribution.MagicKeyTest", enabled = false)
public class MagicKeyTest extends BaseDistFunctionalTest {
   public void testMagicKeys() {
      BaseDistFunctionalTest.MagicKey k1 = new BaseDistFunctionalTest.MagicKey(c1);
      assert getDistributionManager(c1).isLocal(k1);
      assert getDistributionManager(c2).isLocal(k1);
      assert !getDistributionManager(c3).isLocal(k1);
      assert !getDistributionManager(c4).isLocal(k1);

      BaseDistFunctionalTest.MagicKey k2 = new BaseDistFunctionalTest.MagicKey(c2);
      assert !getDistributionManager(c1).isLocal(k2);
      assert getDistributionManager(c2).isLocal(k2);
      assert getDistributionManager(c3).isLocal(k2);
      assert !getDistributionManager(c4).isLocal(k2);

      BaseDistFunctionalTest.MagicKey k3 = new BaseDistFunctionalTest.MagicKey(c3);
      assert !getDistributionManager(c1).isLocal(k3);
      assert !getDistributionManager(c2).isLocal(k3);
      assert getDistributionManager(c3).isLocal(k3);
      assert getDistributionManager(c4).isLocal(k3);

      BaseDistFunctionalTest.MagicKey k4 = new BaseDistFunctionalTest.MagicKey(c4);
      assert getDistributionManager(c1).isLocal(k4);
      assert !getDistributionManager(c2).isLocal(k4);
      assert !getDistributionManager(c3).isLocal(k4);
      assert getDistributionManager(c4).isLocal(k4);
   }
}
