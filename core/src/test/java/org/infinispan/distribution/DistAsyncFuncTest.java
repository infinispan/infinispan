package org.infinispan.distribution;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Predicate;

import org.infinispan.Cache;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.remote.SingleRpcCommand;
import org.infinispan.commands.write.InvalidateL1Command;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.rpc.RpcOptions;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.ReplListener;
import org.infinispan.test.TestingUtil;
import org.infinispan.util.AbstractControlledRpcManager;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

@Test(groups = {"functional"}, testName = "distribution.DistAsyncFuncTest")
public class DistAsyncFuncTest extends DistSyncFuncTest {

   ReplListener r1, r2, r3, r4;
   ReplListener[] r;
   Map<Cache<?, ?>, ReplListener> listenerLookup;
   ConcurrentMap<Cache<?, ?>, List<InvalidateL1Command>> expectedL1Invalidations = new ConcurrentHashMap<>();

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
   protected void createCacheManagers() throws Throwable {
      super.createCacheManagers();
      r1 = new ReplListener(c1, true, true);
      r2 = new ReplListener(c2, true, true);
      r3 = new ReplListener(c3, true, true);
      r4 = new ReplListener(c4, true, true);
      r = new ReplListener[]{r1, r2, r3, r4};
      listenerLookup = new HashMap<>();
      for (ReplListener rl : r) listenerLookup.put(rl.getCache(), rl);

      for (Cache c : caches) {
         TestingUtil.wrapComponent(c, RpcManager.class, original -> new AbstractControlledRpcManager(original) {
            @Override
            public CompletableFuture<Map<Address, Response>> invokeRemotelyAsync(Collection<Address> recipients, ReplicableCommand rpc, RpcOptions options) {
               ReplicableCommand command = rpc;
               if (rpc instanceof SingleRpcCommand) {
                  command = ((SingleRpcCommand) rpc).getCommand();
               }
               if (command instanceof InvalidateL1Command) {
                  InvalidateL1Command invalidateL1Command = (InvalidateL1Command) command;
                  if (recipients != null) {
                     recipients.stream().map(address -> caches.stream()
                           .filter(c -> c.getCacheManager().getAddress().equals(address))
                           .findFirst().<IllegalStateException>orElseThrow(() -> new IllegalStateException("Missing cache for " + address))
                     ).forEach(c -> expectedL1Invalidations
                           .computeIfAbsent(c, ignored -> Collections.synchronizedList(new ArrayList<>()))
                           .add(invalidateL1Command));
                  }
               }
               return super.invokeRemotelyAsync(recipients, rpc, options);
            }
         });
      }
   }

   @AfterMethod
   public void resetEagerCommands() {
      for (ReplListener rl: r) {
         rl.resetEager();
      }
      expectedL1Invalidations.clear();
   }

   @Override
   protected void asyncWait(Object key, Predicate<VisitableCommand> command) {
      if (key == null) {
         // test all caches.
         for (ReplListener rl : r) rl.expect(command);
         for (ReplListener rl : r) rl.waitForRpc();
      } else {
         for (Cache<?, ?> c : getOwners(key)) {
            listenerLookup.get(c).expect(command);
            listenerLookup.get(c).waitForRpc();
         }
      }

      waitForInvalidations();
   }

   private void waitForInvalidations() {
      for (Map.Entry<Cache<?, ?>, List<InvalidateL1Command>> expected : expectedL1Invalidations.entrySet()) {
         ReplListener replListener = listenerLookup.get(expected.getKey());
         List<InvalidateL1Command> list = expected.getValue();
         if (!list.isEmpty()) {
            synchronized (list) {
               for (InvalidateL1Command cmd : list) {
                  replListener.expect(InvalidateL1Command.class);
               }
               list.clear();
            }
            replListener.waitForRpc();
         }
      }
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

      waitForInvalidations();
   }

   @Test(groups = "unstable", description = "ISPN-8298")
   @Override
   public void testLockedStreamSetValue() {
      super.testLockedStreamSetValue();
   }
}
