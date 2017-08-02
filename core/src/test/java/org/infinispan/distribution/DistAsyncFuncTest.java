package org.infinispan.distribution;

import java.util.HashMap;
import java.util.Map;

import org.infinispan.Cache;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.write.InvalidateL1Command;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.test.ReplListener;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

@Test(groups = {"functional", "smoke"}, testName = "distribution.DistAsyncFuncTest")
public class DistAsyncFuncTest extends DistSyncFuncTest {

   @Override
   public Object[] factory() {
      return new Object[] {
         new DistAsyncFuncTest(),
         new DistAsyncFuncTest().groupers(true)
      };
   }

   public DistAsyncFuncTest() {
      cacheMode = CacheMode.DIST_ASYNC;
      testRetVals = false;
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

   @Override
   protected void asyncWaitOnPrimary(Object key, Class<? extends VisitableCommand> command) {
      assert key != null;
      if (key == null) {
         // test all caches.
         for (ReplListener rl : r) rl.expect(command);
         for (ReplListener rl : r) rl.waitForRpc();
      } else {
         Cache<?, ?> primary = getFirstOwner(key);
         listenerLookup.get(primary).expect(command);
         listenerLookup.get(primary).waitForRpc();
      }
      // This sucks but for async transactions we still need this!!
      TestingUtil.sleepThread(1000);
   }
}
