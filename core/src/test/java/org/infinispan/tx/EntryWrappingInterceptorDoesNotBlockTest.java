package org.infinispan.tx;

import static org.infinispan.util.concurrent.CompletableFutures.completedNull;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Stream;

import org.infinispan.Cache;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.remote.ClusteredGetCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.distribution.MagicKey;
import org.infinispan.functional.FunctionalMap;
import org.infinispan.functional.Traversable;
import org.infinispan.functional.impl.FunctionalMapImpl;
import org.infinispan.functional.impl.ReadWriteMapImpl;
import org.infinispan.interceptors.DDAsyncInterceptor;
import org.infinispan.interceptors.InvocationStage;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.TopologyChanged;
import org.infinispan.notifications.cachelistener.event.TopologyChangedEvent;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.rpc.RpcOptions;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.ResponseCollector;
import org.infinispan.statetransfer.StateResponseCommand;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.tx.dld.ControlledRpcManager;
import org.infinispan.util.AbstractControlledRpcManager;
import org.infinispan.util.ControlledConsistentHashFactory;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "tx.EntryWrappingInterceptorDoesNotBlockTest")
@CleanupAfterMethod
public class EntryWrappingInterceptorDoesNotBlockTest extends MultipleCacheManagersTest {
   ConfigurationBuilder cb;
   ControlledConsistentHashFactory chFactory;
   ExecutorService executor = Executors.newCachedThreadPool(getTestThreadFactory("Transport"));

   private static class Operation {
      final String name;
      final BiFunction<MagicKey, Integer, Object> f;

      private Operation(String name, BiFunction<MagicKey, Integer, Object> f) {
         this.name = name;
         this.f = f;
      }

      @Override
      public String toString() {
         return name;
      }
   }

   @DataProvider(name = "operations")
   public Object[][] operations() {
      return Stream.of(
            new Operation("readWriteKey", this::readWriteKey),
            new Operation("readWriteKeyValue", this::readWriteKeyValue),
            new Operation("readWriteMany", this::readWriteMany),
            new Operation("readWriteManyEntries", this::readWriteManyEntries)
      ).map(f -> new Object[] { f }).toArray(Object[][]::new);
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      chFactory = new ControlledConsistentHashFactory.Default(new int[][]{{0, 1}, {0, 2}});
      cb = new ConfigurationBuilder();
      cb.clustering().cacheMode(CacheMode.DIST_SYNC).hash().consistentHashFactory(chFactory).numSegments(2);
      cb.transaction().transactionMode(TransactionMode.TRANSACTIONAL);
      createCluster(cb, 3);
      // make sure the caches are started in proper order
      for (int i = 0; i < 3; ++i) cache(i);
   }

   @Test(dataProvider = "operations")
   public void testMovingStable(Operation operation) throws Exception {
      test(1, operation.f, new MagicKey("moving", cache(0), cache(1)), new MagicKey("stable", cache(0), cache(2)));
   }

   @Test(dataProvider = "operations")
   public void testStableMoving(Operation operation) throws Exception {
      test(1, operation.f, new MagicKey("stable", cache(0), cache(2)), new MagicKey("moving", cache(0), cache(1)));
   }

   @Test(dataProvider = "operations")
   public void testMovingMoving(Operation operation) throws Exception {
      test(2, operation.f, new MagicKey("moving1", cache(0), cache(1)), new MagicKey("moving2", cache(0), cache(1)));
   }

   protected void test(int expectRemoteGets, BiFunction<MagicKey, Integer, Object> operation, MagicKey... keys) throws Exception {
      ControlledRpcManager crm0 = replace(cache(0), ControlledRpcManager::new);
      ControlledRpcManager crm1 = replace(cache(1), ControlledRpcManager::new);
      CountDownLatch prepareLatch = new CountDownLatch(1);
      CountingBlockingRpcManager crm2 = replace(cache(2), delegate -> new CountingBlockingRpcManager(delegate, prepareLatch));
      CountDownLatch topologyChangeLatch = new CountDownLatch(2);
      cache(0).addListener(new TopologyChangeListener(topologyChangeLatch));
      cache(2).addListener(new TopologyChangeListener(topologyChangeLatch));
      cache(2).getAdvancedCache().getAsyncInterceptorChain().addInterceptor(new PrepareExpectingInterceptor(prepareLatch), 0);

      tm(0).begin();
      for (int i = 0; i < keys.length; ++i) {
         Object returnValue = operation.apply(keys[i], i);
         assertEquals("r" + i, returnValue);
      }

      // node 2 should be backup for both segment 0
      chFactory.setOwnerIndexes(new int[][]{{0, 2}, {0, 2}});
      // block sending segment 0 to node 2
      crm0.blockBefore(StateResponseCommand.class);
      crm1.blockBefore(StateResponseCommand.class);

      addClusterEnabledCacheManager(cb);
      Future<?> newNode = fork(() -> cache(3));
      assertTrue(topologyChangeLatch.await(10, TimeUnit.SECONDS));

      tm(0).commit();

      // the node should load all moving keys
      assertEquals(expectRemoteGets, crm2.clusterGet);
      for (int i = 0; i < keys.length; i++) {
         MagicKey key = keys[i];
         assertEquals("v" + i, cache(2).get(key));
      }

      crm0.stopBlocking();
      crm1.stopBlocking();

      newNode.get(10, TimeUnit.SECONDS);
   }

   private Object readWriteKey(MagicKey key, int index) {
      FunctionalMap.ReadWriteMap<Object, Object> rwMap = ReadWriteMapImpl.create(FunctionalMapImpl.create(cache(0).getAdvancedCache()));
      CompletableFuture cf = rwMap.eval(key, view -> {
         assertFalse(view.find().isPresent());
         view.set("v" + index);
         return "r" + index;
      });
      return cf.join();
   }

   private Object readWriteMany(MagicKey key, int index) {
      // make sure the other key is stable
      MagicKey otherKey = new MagicKey("other", cache(0), cache(2));
      FunctionalMap.ReadWriteMap<Object, Object> rwMap = ReadWriteMapImpl.create(FunctionalMapImpl.create(cache(0).getAdvancedCache()));
      HashSet<MagicKey> keys = new HashSet<>(Arrays.asList(key, otherKey));
      Traversable<Object> traversable = rwMap.evalMany(keys, view -> {
         assertFalse(view.find().isPresent());
         view.set("v" + index);
         return "r" + index;
      });
      return traversable.findAny().orElseThrow(IllegalStateException::new);
   }

   private Object readWriteKeyValue(MagicKey key, int index) {
      FunctionalMap.ReadWriteMap<Object, Object> rwMap = ReadWriteMapImpl.create(FunctionalMapImpl.create(cache(0).getAdvancedCache()));
      CompletableFuture cfa = rwMap.eval(key, "v" + index, (value, view) -> {
         assertFalse(view.find().isPresent());
         view.set(value);
         return "r" + index;
      });
      return cfa.join();
   }

   private Object readWriteManyEntries(MagicKey key, int index) {
      // make sure the other key is stable
      MagicKey otherKey = new MagicKey("other", cache(0), cache(2));
      FunctionalMap.ReadWriteMap<Object, Object> rwMap = ReadWriteMapImpl.create(FunctionalMapImpl.create(cache(0).getAdvancedCache()));
      HashMap<MagicKey, Object> map = new HashMap<>();
      map.put(key, "v" + index);
      map.put(otherKey, "something");
      Traversable<Object> traversable = rwMap.evalMany(map, (value, view) -> {
         assertFalse(view.find().isPresent());
         view.set(value);
         return "r" + index;
      });
      return traversable.findAny().orElseThrow(IllegalStateException::new);
   }

   private static <T extends RpcManager> T replace(Cache<Object, Object> cache, Function<RpcManager, T> ctor) {
      T crm = ctor.apply(TestingUtil.extractComponent(cache, RpcManager.class));
      TestingUtil.replaceComponent(cache, RpcManager.class, crm, true);
      return crm;
   }

   @Listener(observation = Listener.Observation.POST)
   private class TopologyChangeListener {
      private final CountDownLatch latch;

      public TopologyChangeListener(CountDownLatch latch) {
         this.latch = latch;
      }

      @TopologyChanged
      public void onTopologyChange(TopologyChangedEvent event) {
         latch.countDown();
      }
   }

   private class CountingBlockingRpcManager extends AbstractControlledRpcManager {
      private final CountDownLatch latch;
      private int clusterGet;

      public CountingBlockingRpcManager(RpcManager delegate, CountDownLatch latch) {
         super(delegate);
         this.latch = latch;
      }

      @Override
      public <T> CompletionStage<T> invokeCommand(Address target, ReplicableCommand command,
                                                  ResponseCollector<T> collector, RpcOptions rpcOptions) {
         // We have to force afterInvokeRemotely being invoked in another thread, because if the response
         // arrives too soon, we could be processing in the same thread that is about to wait for the prepare
         // command to finish without blocking
         return completedNull()
               .thenComposeAsync(o -> super.invokeCommand(target, command, collector, rpcOptions), executor);
      }

      @Override
      public <T> CompletionStage<T> invokeCommand(Collection<Address> targets, ReplicableCommand command,
                                                  ResponseCollector<T> collector, RpcOptions rpcOptions) {
         // We have to force afterInvokeRemotely being invoked in another thread, because if the response
         // arrives too soon, we could be processing in the same thread that is about to wait for the prepare
         // command to finish without blocking
         return completedNull()
               .thenComposeAsync(o -> super.invokeCommand(targets, command, collector, rpcOptions), executor);
      }

      @Override
      public <T> CompletionStage<T> invokeCommandOnAll(ReplicableCommand command, ResponseCollector<T> collector,
                                                       RpcOptions rpcOptions) {
         // We have to force afterInvokeRemotely being invoked in another thread, because if the response
         // arrives too soon, we could be processing in the same thread that is about to wait for the prepare
         // command to finish without blocking
         return completedNull()
               .thenComposeAsync(o -> super.invokeCommandOnAll(command, collector, rpcOptions), executor);
      }

      @Override
      public <T> CompletionStage<T> invokeCommandStaggered(Collection<Address> targets, ReplicableCommand command,
                                                           ResponseCollector<T> collector, RpcOptions rpcOptions) {
         // We have to force afterInvokeRemotely being invoked in another thread, because if the response
         // arrives too soon, we could be processing in the same thread that is about to wait for the prepare
         // command to finish without blocking
         return completedNull()
               .thenComposeAsync(o -> super.invokeCommandStaggered(targets, command, collector, rpcOptions), executor);
      }

      @Override
      protected <T> T afterInvokeRemotely(ReplicableCommand command, T responseObject, Object argument) {
         if (command instanceof ClusteredGetCommand) {
            ++clusterGet;
            try {
               log.debug("Waiting until PrepareExpectingInterceptor gets incomplete stage");
               assertTrue(latch.await(10, TimeUnit.SECONDS));
               log.debug("Releasing read response");
            } catch (InterruptedException e) {
               Thread.currentThread().interrupt();
               throw new RuntimeException(e);
            }
         }
         return responseObject;
      }
   }

   private class PrepareExpectingInterceptor extends DDAsyncInterceptor {
      private final CountDownLatch latch;

      public PrepareExpectingInterceptor(CountDownLatch latch) {
         this.latch = latch;
      }

      @Override
      public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
         assertFalse(ctx.isOriginLocal());
         InvocationStage invocationStage = makeStage(invokeNext(ctx, command));
         assertFalse(invocationStage.toString(), invocationStage.isDone());
         log.debug("Received incomplete stage");
         latch.countDown();
         return invocationStage;
      }
   }
}
