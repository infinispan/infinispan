package org.infinispan.util;

import org.infinispan.commons.hash.Hash;
import org.infinispan.commons.hash.MurmurHash2;
import org.infinispan.commons.hash.MurmurHash3;
import org.infinispan.test.AbstractInfinispanTest;
import org.testng.annotations.Test;

@Test(testName = "util.HashFunctionTest", groups = "unit")
public class HashFunctionTest extends AbstractInfinispanTest {

   public void testMurmurHash2Consistency() {
      testHashConsistency(new MurmurHash2());
   }

   public void testMurmurHash3Consistency() {
      testHashConsistency(MurmurHash3.getInstance());
   }

   private void testHashConsistency(Hash hash) {
      Object o = new Object();
      int i1 = hash.hash(o);
      int i2 = hash.hash(o);
      int i3 = hash.hash(o);

      assert i1 == i2: "i1 and i2 are not the same: " + i1 + ", " + i2;
      assert i3 == i2: "i3 and i2 are not the same: " + i2 + ", " + i3;
      assert i1 == i3: "i1 and i3 are not the same: " + i1 + ", " + i3;
   }

}
