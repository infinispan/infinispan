package org.infinispan.distribution;

import org.infinispan.Cache;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.write.InvalidateCommand;
import org.infinispan.test.ReplListener;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

@Test(groups = "functional", testName = "distribution.DistAsyncTxFuncTest")
public class DistAsyncTxFuncTest extends DistSyncTxFuncTest {

   ReplListener r1, r2, r3, r4;
   ReplListener[] r;
   Map<Cache<?, ?>, ReplListener> listenerLookup;

   public DistAsyncTxFuncTest() {
      sync = false;
      tx = true;
      cleanup = CleanupPhase.AFTER_METHOD; // ensure any stale TXs are wiped
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
               listenerLookup.get(c).expect(InvalidateCommand.class);
               listenerLookup.get(c).waitForRpc();
            }
         }
      }
   }

   @Override
   protected void asyncTxWait(Object... keys) {
      // Wait for a tx completion event
      if (keys != null) {
         Set<Cache<?, ?>> cachesInTx = new HashSet<Cache<?, ?>>();
         for (Object k : keys) {
            cachesInTx.addAll(Arrays.asList(getOwners(k)));
         }

         for (Cache<?, ?> c : cachesInTx) {
            listenerLookup.get(c).expectAnyWithTx();
            listenerLookup.get(c).waitForRpc();
         }
      }
   }
}