package org.infinispan.distribution;

import static org.infinispan.test.TestingUtil.k;
import static org.infinispan.test.TestingUtil.v;

import java.lang.reflect.Method;

import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "distribution.DisabledL1Test")
public class DisabledL1Test extends BaseDistFunctionalTest<Object, String> {

   public DisabledL1Test() {
      testRetVals = false;
      l1CacheEnabled = false;
   }

   public void testRemoveFromNonOwner() {
      Object retval = getFirstNonOwner("k1").put("k1", "value");
      asyncWait("k1", PutKeyValueCommand.class);
      if (testRetVals) assert retval == null;

      retval = getOwners("k1")[0].remove("k1");
      asyncWait("k1", RemoveCommand.class);
      if (testRetVals) assert "value".equals(retval);

      assertRemovedOnAllCaches("k1");
   }

   public void testReplaceFromNonOwner(Method m) {
      final String k = k(m);
      final String v = v(m);
      getOwners(k)[0].put(k, v);
      getNonOwners(k)[0].replace(k, v(m, 1));
   }
}
