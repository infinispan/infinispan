package org.infinispan.distribution;

import org.testng.annotations.Test;

@Test(groups = "unit", testName = "distribution.MagicKeyTest")
public class MagicKeyTest extends BaseDistFunctionalTest<Object, String> {
   public void testMagicKeys() {
      MagicKey k1 = new MagicKey(c1, c2);
      assert getCacheTopology(c1).isWriteOwner(k1);
      assert getCacheTopology(c2).isWriteOwner(k1);
      assert !getCacheTopology(c3).isWriteOwner(k1);
      assert !getCacheTopology(c4).isWriteOwner(k1);

      MagicKey k2 = new MagicKey(c2, c3);
      assert !getCacheTopology(c1).isWriteOwner(k2);
      assert getCacheTopology(c2).isWriteOwner(k2);
      assert getCacheTopology(c3).isWriteOwner(k2);
      assert !getCacheTopology(c4).isWriteOwner(k2);

      MagicKey k3 = new MagicKey(c3, c4);
      assert !getCacheTopology(c1).isWriteOwner(k3);
      assert !getCacheTopology(c2).isWriteOwner(k3);
      assert getCacheTopology(c3).isWriteOwner(k3);
      assert getCacheTopology(c4).isWriteOwner(k3);

      MagicKey k4 = new MagicKey(c4, c1);
      assert getCacheTopology(c1).isWriteOwner(k4);
      assert !getCacheTopology(c2).isWriteOwner(k4);
      assert !getCacheTopology(c3).isWriteOwner(k4);
      assert getCacheTopology(c4).isWriteOwner(k4);
   }
}
