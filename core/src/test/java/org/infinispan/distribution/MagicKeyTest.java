package org.infinispan.distribution;

import org.testng.annotations.Test;

@Test(groups = "unit", testName = "distribution.MagicKeyTest")
public class MagicKeyTest extends BaseDistFunctionalTest {
   public void testMagicKeys() {
      MagicKey k1 = new MagicKey(c1);
      assert getDistributionManager(c1).isLocal(k1);
      assert getDistributionManager(c2).isLocal(k1);
      assert !getDistributionManager(c3).isLocal(k1);
      assert !getDistributionManager(c4).isLocal(k1);

      MagicKey k2 = new MagicKey(c2);
      assert !getDistributionManager(c1).isLocal(k2);
      assert getDistributionManager(c2).isLocal(k2);
      assert getDistributionManager(c3).isLocal(k2);
      assert !getDistributionManager(c4).isLocal(k2);

      MagicKey k3 = new MagicKey(c3);
      assert !getDistributionManager(c1).isLocal(k3);
      assert !getDistributionManager(c2).isLocal(k3);
      assert getDistributionManager(c3).isLocal(k3);
      assert getDistributionManager(c4).isLocal(k3);

      MagicKey k4 = new MagicKey(c4);
      assert getDistributionManager(c1).isLocal(k4);
      assert !getDistributionManager(c2).isLocal(k4);
      assert !getDistributionManager(c3).isLocal(k4);
      assert getDistributionManager(c4).isLocal(k4);
   }
}
