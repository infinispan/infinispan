package org.infinispan.distribution;

import java.util.ArrayList;
import java.util.Collection;

import org.infinispan.Cache;
import org.infinispan.commands.write.InvalidateL1Command;
import org.infinispan.test.ReplListener;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "distribution.DistSyncFuncTest")
public class MulticastInvalidationFuncTest extends BaseDistFunctionalTest {
	
	public static final String KEY1 = "k1";

   public MulticastInvalidationFuncTest() {
      sync = true;
      tx = false;
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
      Assert.assertEquals(nonOwner.getAdvancedCache().getDataContainer().get(KEY1).getValue(), "foo");
      Assert.assertEquals(owner.getAdvancedCache().getDataContainer().get(KEY1).getValue(), "foo");
      
      // Check that all nodes are notified
      for (Cache<Object, String> c : getNonOwners(KEY1)) {
   		ReplListener rl = new ReplListener(c);
         rl.expect(InvalidateL1Command.class);
         listeners.add(rl);
      }
      
      // Put an object into an owner, this will cause the L1 records for this key to be invalidated
      owner.put(KEY1, "bar");
      
      for (ReplListener rl : listeners) {
      	rl.waitForRpc();
      }
      
      Assert.assertNull(nonOwner.getAdvancedCache().getDataContainer().get(KEY1));
      
      
   }
   
}
