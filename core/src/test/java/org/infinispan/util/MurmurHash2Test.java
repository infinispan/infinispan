package org.infinispan.util;

import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.util.hash.MurmurHash2;
import org.testng.annotations.Test;

/**
 * TODO: Document this
 *
 * @author Manik Surtani
 * @version 4.1
 */
@Test(testName = "util.MurmurHash2Test", groups = "unit")
public class MurmurHash2Test extends AbstractInfinispanTest {

   public void testHashConsistency() {
      Object o = new Object();
      int i1 = MurmurHash2.hash(o);
      int i2 = MurmurHash2.hash(o);
      int i3 = MurmurHash2.hash(o);

      assert i1 == i2: "i1 and i2 are not the same: " + i1 + ", " + i2;
      assert i3 == i2: "i3 and i2 are not the same: " + i2 + ", " + i3;
      assert i1 == i3: "i1 and i3 are not the same: " + i1 + ", " + i3;
   }

}
