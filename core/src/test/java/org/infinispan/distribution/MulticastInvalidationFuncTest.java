package org.infinispan.distribution;

import static org.testng.AssertJUnit.assertNull;

import java.util.ArrayList;
import java.util.Collection;

import org.infinispan.Cache;
import org.infinispan.commands.write.InvalidateL1Command;
import org.infinispan.test.ReplListener;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "distribution.MulticastInvalidationFuncTest")
public class MulticastInvalidationFuncTest extends BaseDistFunctionalTest<Object, String> {

   public static final String KEY1 = "k1";

   public MulticastInvalidationFuncTest() {
      testRetVals = true;
      l1Threshold = 0;
   }

   public void testPut() {
      initAndTest();
      Cache<Object, String> nonOwner = getFirstNonOwner(KEY1);
      Cache<Object, String> owner = getOwners(KEY1)[0];
      Collection<ReplListener> listeners = new ArrayList<ReplListener>();

      // Put an object in from a non-owner, this will cause an L1 record to be created there

      nonOwner.put(KEY1, "foo");
      assertNull(nonOwner.getAdvancedCache().getDataContainer().peek(KEY1));
      Assert.assertEquals(owner.getAdvancedCache().getDataContainer().peek(KEY1).getValue(), "foo");

      // Check that all nodes (except the one we put to) are notified
      // but only if the transport is multicast-capable
      if (owner.getAdvancedCache().getRpcManager().getTransport().isMulticastCapable()) {
         for (Cache<Object, String> c : getNonOwners(KEY1)) {
            ReplListener rl = new ReplListener(c);
            rl.expect(InvalidateL1Command.class);
            listeners.add(rl);
            log.debugf("Added nonowner %s", c);
         }
      } else {
         ReplListener rl = new ReplListener(nonOwner);
         rl.expect(InvalidateL1Command.class);
         listeners.add(rl);
      }

      // Put an object into an owner, this will cause the L1 records for this key to be invalidated
      owner.put(KEY1, "bar");

      for (ReplListener rl : listeners) {
         rl.waitForRpc();
      }

      Assert.assertNull(nonOwner.getAdvancedCache().getDataContainer().peek(KEY1));
   }

}
