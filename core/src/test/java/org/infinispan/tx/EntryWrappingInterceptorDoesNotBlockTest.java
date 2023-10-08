package org.infinispan.tx;

import static org.infinispan.test.fwk.TestCacheManagerFactory.createClusteredCacheManager;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Stream;

import org.infinispan.Cache;
import org.infinispan.commands.remote.BaseClusteredReadCommand;
import org.infinispan.commands.remote.ClusteredGetCommand;
import org.infinispan.commands.remote.recovery.TxCompletionNotificationCommand;
import org.infinispan.commands.statetransfer.StateResponseCommand;
import org.infinispan.commands.statetransfer.StateTransferGetTransactionsCommand;
import org.infinispan.commands.statetransfer.StateTransferStartCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commons.TimeoutException;
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
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.TopologyChanged;
import org.infinispan.notifications.cachelistener.event.TopologyChangedEvent;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.test.fwk.TransportFlags;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.util.ControlledConsistentHashFactory;
import org.infinispan.util.ControlledRpcManager;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import jakarta.transaction.Transaction;

/**
 * We make node 2 a backup owner for segment 0, but we block state transfer so that it doesn't have any entries.
 * We verify that when the prepare command is invoked remotely on node 2, a remote get is sent to nodes 0 and 1,
 * and that the remote thread is not blocked while waiting for the remote get responses.
 */
@Test(groups = "functional", testName = "tx.EntryWrappingInterceptorDoesNotBlockTest")
@CleanupAfterMethod
public class EntryWrappingInterceptorDoesNotBlockTest extends MultipleCacheManagersTest {
   private ConfigurationBuilder cb;
   private ControlledConsistentHashFactory.Default chFactory;

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
      createCluster(ControlledConsistentHashFactory.SCI.INSTANCE, cb, 3);
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
      ControlledRpcManager crm0 = ControlledRpcManager.replaceRpcManager(cache(0));
      ControlledRpcManager crm2 = ControlledRpcManager.replaceRpcManager(cache(2));
      CountDownLatch topologyChangeLatch = new CountDownLatch(2);
      cache(0).addListener(new TopologyChangeListener(topologyChangeLatch));
      cache(2).addListener(new TopologyChangeListener(topologyChangeLatch));
      PrepareExpectingInterceptor prepareExpectingInterceptor = new PrepareExpectingInterceptor();
      TestingUtil.extractInterceptorChain(cache(2)).addInterceptor(prepareExpectingInterceptor, 0);

      tm(0).begin();
      for (int i = 0; i < keys.length; ++i) {
         Object returnValue = operation.apply(keys[i], i);
         assertEquals("r" + i, returnValue);
      }

      // node 2 should become backup for both segment 0 (new) and segment 1 (already there)
      chFactory.setOwnerIndexes(new int[][]{{0, 2}, {0, 2}});

      EmbeddedCacheManager cm = createClusteredCacheManager(false, ControlledConsistentHashFactory.SCI.INSTANCE, cb, new TransportFlags());
      registerCacheManager(cm);
      Future<?> newNode = fork(() -> cache(3));

      // block sending segment 0 to node 2
      crm2.expectCommand(StateTransferGetTransactionsCommand.class).send().receiveAll();
      crm2.expectCommand(StateTransferStartCommand.class).send().receiveAllAsync();
      ControlledRpcManager.BlockedRequest blockedStateResponse0 = crm0.expectCommand(StateResponseCommand.class);
      assertTrue(topologyChangeLatch.await(10, TimeUnit.SECONDS));

      Transaction transaction = tm(0).suspend();
      Future<Void> commitFuture = fork(transaction::commit);

      ControlledRpcManager.SentRequest sentPrepare = crm0.expectCommand(PrepareCommand.class).send();

      // The PrepareCommand attempts to load moving keys, and we allow the request to be sent
      // but block receiving the responses. Here we'll intercept only the first remote get because the second one
      // is not fired until the first is received (implementation inefficiency).
      ControlledRpcManager.SentRequest firstRemoteGet = crm2.expectCommand(BaseClusteredReadCommand.class).send();

      // The topmost interceptor gets the InvocationStage from the PrepareCommand and verifies
      // that it is not completed yet (as we are waiting for the remote gets). Receiving the invocation stage
      // means that the stack is really non-blocking.
      // If the remote get responses hadn't been blocked this verification would fail with assertion.
      prepareExpectingInterceptor.await();

      // Receiving the responses for one remote get triggers the next remote get, so complete them in parallel
      firstRemoteGet.expectAllResponses().receiveAsync();
      for (int i = 1; i < expectRemoteGets; ++i) {
         crm2.expectCommand(BaseClusteredReadCommand.class).send().receiveAll();
      }

      sentPrepare.expectAllResponses().receiveAsync();
      crm0.expectCommand(CommitCommand.class).send().receiveAll();
      crm0.expectCommand(TxCompletionNotificationCommand.class).send();

      commitFuture.get(10, TimeUnit.SECONDS);

      crm2.excludeCommands(ClusteredGetCommand.class);
      for (int i = 0; i < keys.length; i++) {
         MagicKey key = keys[i];
         assertEquals("v" + i, cache(2).get(key));
      }

      blockedStateResponse0.send().receiveAll();

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
   private static class TopologyChangeListener {
      private final CountDownLatch latch;

      TopologyChangeListener(CountDownLatch latch) {
         this.latch = latch;
      }

      @TopologyChanged
      public void onTopologyChange(TopologyChangedEvent event) {
         latch.countDown();
      }
   }

   static class PrepareExpectingInterceptor extends DDAsyncInterceptor {
      private final CountDownLatch latch = new CountDownLatch(1);

      @Override
      public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
         assertFalse(ctx.isOriginLocal());
         InvocationStage invocationStage = makeStage(invokeNext(ctx, command));
         assertFalse(invocationStage.toString(), invocationStage.isDone());
         log.debug("Received incomplete stage");
         latch.countDown();
         return invocationStage;
      }

      public void await() throws InterruptedException {
         boolean success = latch.await(10, TimeUnit.SECONDS);
         if (!success) {
            throw new TimeoutException("Timed out waiting for PrepareCommand");
         }
      }
   }
}
