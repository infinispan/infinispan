package org.infinispan.distribution;

import org.infinispan.Cache;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.write.InvalidateL1Command;
import org.infinispan.test.ReplListener;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

@Test(groups = {"functional", "smoke"}, testName = "distribution.DistAsyncFuncTest")
public class DistAsyncFuncTest extends DistSyncFuncTest {

   ReplListener r1, r2, r3, r4;
   ReplListener[] r;
   Map<Cache<?, ?>, ReplListener> listenerLookup;

   public DistAsyncFuncTest() {
      sync = false;
      tx = false;
      testRetVals = false;
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      super.createCacheManagers();
      r1 = new ReplListener(c1, true, true);
      r2 = new ReplListener(c2, true, true);
      r3 = new ReplListener(c3, true, true);
      r4 = new ReplListener(c4, true, true);
      r = new ReplListener[]{r1, r2, r3, r4};
      listenerLookup = new HashMap<Cache<?, ?>, ReplListener>();
      for (ReplListener rl : r) listenerLookup.put(rl.getCache(), rl);
   }

   @Override
   protected void asyncWait(Object key, Class<? extends VisitableCommand> command, Cache<?, ?>... cachesOnWhichKeyShouldInval) {
      if (key == null) {
         // test all caches.
         for (ReplListener rl : r) rl.expect(command);
         for (ReplListener rl : r) rl.waitForRpc();
      } else {
         for (Cache<?, ?> c : getOwners(key)) {
            listenerLookup.get(c).expect(command);
            listenerLookup.get(c).waitForRpc();
         }

         if (cachesOnWhichKeyShouldInval != null) {
            for (Cache<?, ?> c : cachesOnWhichKeyShouldInval) {
               listenerLookup.get(c).expect(InvalidateL1Command.class);
               listenerLookup.get(c).waitForRpc();
            }
         }
      }
      // This sucks but for async transactions we still need this!!
      TestingUtil.sleepThread(1000);
   }
}