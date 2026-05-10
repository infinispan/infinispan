package org.infinispan.distribution;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.testng.annotations.Test;

@Test(groups = "unit", testName = "distribution.MagicKeyTest")
public class MagicKeyTest extends BaseDistFunctionalTest<Object, String> {
   public void testMagicKeys() {
      MagicKey k1 = new MagicKey(c1, c2);
      assertTrue(getCacheTopology(c1).isWriteOwner(k1));
      assertTrue(getCacheTopology(c2).isWriteOwner(k1));
      assertFalse(getCacheTopology(c3).isWriteOwner(k1));
      assertFalse(getCacheTopology(c4).isWriteOwner(k1));

      MagicKey k2 = new MagicKey(c2, c3);
      assertFalse(getCacheTopology(c1).isWriteOwner(k2));
      assertTrue(getCacheTopology(c2).isWriteOwner(k2));
      assertTrue(getCacheTopology(c3).isWriteOwner(k2));
      assertFalse(getCacheTopology(c4).isWriteOwner(k2));

      MagicKey k3 = new MagicKey(c3, c4);
      assertFalse(getCacheTopology(c1).isWriteOwner(k3));
      assertFalse(getCacheTopology(c2).isWriteOwner(k3));
      assertTrue(getCacheTopology(c3).isWriteOwner(k3));
      assertTrue(getCacheTopology(c4).isWriteOwner(k3));

      MagicKey k4 = new MagicKey(c4, c1);
      assertTrue(getCacheTopology(c1).isWriteOwner(k4));
      assertFalse(getCacheTopology(c2).isWriteOwner(k4));
      assertFalse(getCacheTopology(c3).isWriteOwner(k4));
      assertTrue(getCacheTopology(c4).isWriteOwner(k4));
   }
}
