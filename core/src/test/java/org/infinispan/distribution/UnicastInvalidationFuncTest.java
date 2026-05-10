package org.infinispan.distribution;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.ArrayList;
import java.util.Collection;

import org.infinispan.Cache;
import org.infinispan.commands.write.InvalidateL1Command;
import org.infinispan.test.ReplListener;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "distribution.UnicastInvalidationFuncTest")
public class UnicastInvalidationFuncTest extends BaseDistFunctionalTest<Object, String> {

   public static final String KEY1 = "k1";

   public UnicastInvalidationFuncTest() {
      testRetVals = true;
      l1Threshold = -1;
   }

   public void testPut() {
      initAndTest();
      Cache<Object, String> nonOwner = getFirstNonOwner(KEY1);
      Cache<Object, String> owner = getOwners(KEY1)[0];
      Cache<Object, String> secondNonOwner = getSecondNonOwner(KEY1);
      Collection<ReplListener> listeners = new ArrayList<ReplListener>();

      // Put an object in from a non-owner, this will cause an L1 record to be created there

      nonOwner.put(KEY1, "foo");
      assertNull(nonOwner.getAdvancedCache().getDataContainer().peek(KEY1));
      assertEquals("foo", owner.getAdvancedCache().getDataContainer().peek(KEY1).getValue());

      // Request from another non-owner so that we can get an invalidation command there
      assertEquals("foo", secondNonOwner.get(KEY1));
      assertEquals("foo", secondNonOwner.getAdvancedCache().getDataContainer().peek(KEY1).getValue());

      // Check that the non owners are notified
      ReplListener rl = new ReplListener(nonOwner);
      rl.expect(InvalidateL1Command.class);
      listeners.add(rl);
      rl = new ReplListener(secondNonOwner);
      rl.expect(InvalidateL1Command.class);
      listeners.add(rl);

      // Put an object into an owner, this will cause the L1 records for this key to be invalidated
      owner.put(KEY1, "bar");

      for (ReplListener r : listeners) {
         r.waitForRpc();
      }

      assertNull(secondNonOwner.getAdvancedCache().getDataContainer().peek(KEY1));
      assertNull(nonOwner.getAdvancedCache().getDataContainer().peek(KEY1));


   }

}
