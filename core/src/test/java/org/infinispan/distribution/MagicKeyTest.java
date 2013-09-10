package org.infinispan.distribution;

import org.testng.annotations.Test;

@Test(groups = "unit", testName = "distribution.MagicKeyTest")
public class MagicKeyTest extends BaseDistFunctionalTest<Object, String> {
   public void testMagicKeys() {
      MagicKey k1 = new MagicKey(c1, c2);
      assert getDistributionManager(c1).getLocality(k1).isLocal();
      assert getDistributionManager(c2).getLocality(k1).isLocal();
      assert !getDistributionManager(c3).getLocality(k1).isLocal();
      assert !getDistributionManager(c4).getLocality(k1).isLocal();

      MagicKey k2 = new MagicKey(c2, c3);
      assert !getDistributionManager(c1).getLocality(k2).isLocal();
      assert getDistributionManager(c2).getLocality(k2).isLocal();
      assert getDistributionManager(c3).getLocality(k2).isLocal();
      assert !getDistributionManager(c4).getLocality(k2).isLocal();

      MagicKey k3 = new MagicKey(c3, c4);
      assert !getDistributionManager(c1).getLocality(k3).isLocal();
      assert !getDistributionManager(c2).getLocality(k3).isLocal();
      assert getDistributionManager(c3).getLocality(k3).isLocal();
      assert getDistributionManager(c4).getLocality(k3).isLocal();

      MagicKey k4 = new MagicKey(c4, c1);
      assert getDistributionManager(c1).getLocality(k4).isLocal();
      assert !getDistributionManager(c2).getLocality(k4).isLocal();
      assert !getDistributionManager(c3).getLocality(k4).isLocal();
      assert getDistributionManager(c4).getLocality(k4).isLocal();
   }
}
