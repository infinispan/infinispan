package org.infinispan.distribution;

import static org.testng.AssertJUnit.assertNull;

import org.infinispan.Cache;
import org.infinispan.context.Flag;
import org.testng.annotations.Test;

@Test(groups = {"functional", "smoke"}, testName = "distribution.DistSinglePutTest")
public class DistSinglePutTest extends BaseDistFunctionalTest<Object, String> {

   public DistSinglePutTest() {
      INIT_CLUSTER_SIZE = 3;
   }

   @Override
   protected Cache<Object, String> getFirstNonOwner(Object key) {
      return super.getFirstNonOwner(key).getAdvancedCache().withFlags(Flag.IGNORE_RETURN_VALUES);
   }

   public void testPutFromNonOwner() {
      Cache<Object, String> nonOwner = getFirstNonOwner("k1");
      Object retval = nonOwner.put("k1", "value2");
      assertNull(retval);
      assertOnAllCachesAndOwnership("k1", "value2");
   }

}
