package org.infinispan.stats;

import org.infinispan.Cache;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.commands.remote.SingleRpcCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.VersionedPrepareCommand;
import org.infinispan.commands.tx.totalorder.TotalOrderNonVersionedPrepareCommand;
import org.infinispan.commands.tx.totalorder.TotalOrderVersionedPrepareCommand;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.InterceptorConfiguration;
import org.infinispan.configuration.cache.VersioningScheme;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.interceptors.SequentialInterceptorChain;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.inboundhandler.PerCacheInboundInvocationHandler;
import org.infinispan.remoting.inboundhandler.Reply;
import org.infinispan.remoting.transport.Address;
import org.infinispan.stats.wrappers.ExtendedStatisticInterceptor;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.transaction.TransactionProtocol;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.infinispan.test.TestingUtil.extractComponent;
import static org.infinispan.test.TestingUtil.k;
import static org.infinispan.test.TestingUtil.replaceComponent;
import static org.infinispan.test.TestingUtil.replaceField;
import static org.testng.AssertJUnit.assertNull;

/**
 * @author Pedro Ruivo
 * @since 6.0
 */
@Test(groups = "functional")
public abstract class BaseClusteredExtendedStatisticTest extends MultipleCacheManagersTest {

   private static final int NUM_NODES = 2;
   private static final String VALUE_1 = "value_1";
   private static final String VALUE_2 = "value_2";
   private static final String VALUE_3 = "value_3";
   private static final String VALUE_4 = "value_4";
   private final List<ControlledPerCacheInboundInvocationHandler> inboundHandlerList = new ArrayList<>(NUM_NODES);
   private final CacheMode mode;
   private final boolean sync2ndPhase;
   private final boolean writeSkew;
   private final boolean totalOrder;

   protected BaseClusteredExtendedStatisticTest(CacheMode mode, boolean sync2ndPhase, boolean writeSkew,
                                                boolean totalOrder) {
      this.mode = mode;
      this.sync2ndPhase = sync2ndPhase;
      this.writeSkew = writeSkew;
      this.totalOrder = totalOrder;
   }

   protected static Collection<Address> getOwners(Cache<?, ?> cache, Object key) {
      return new ArrayList<>(cache.getAdvancedCache().getDistributionManager().locate(key));
   }

   protected static Collection<Address> getOwners(Cache<?, ?> cache, Collection<Object> keys) {
      return new ArrayList<>(cache.getAdvancedCache().getDistributionManager().locateAll(keys));
   }

   public void testPut(Method method) throws InterruptedException {
      final String key1 = k(method, 1);
      final String key2 = k(method, 2);
      final String key3 = k(method, 3);
      assertEmpty(key1, key2, key3);

      put(0, key1, VALUE_1);

      assertCacheValue(key1, VALUE_1);

      Map<Object, Object> map = new HashMap<>();
      map.put(key2, VALUE_2);
      map.put(key3, VALUE_3);

      cache(0).putAll(map);
      awaitPutMap(0, map.keySet());

      assertCacheValue(key1, VALUE_1);
      assertCacheValue(key2, VALUE_2);
      assertCacheValue(key3, VALUE_3);

      assertNoTransactions();
      assertNoTxStats();
   }

   public void testRemove(Method method) throws InterruptedException {
      final String key1 = k(method, 1);
      assertEmpty(key1);

      put(1, key1, VALUE_1);

      assertCacheValue(key1, VALUE_1);

      remove(0, key1);

      assertCacheValue(key1, null);

      put(0, key1, VALUE_1);

      assertCacheValue(key1, VALUE_1);

      remove(0, key1);

      assertCacheValue(key1, null);

      assertNoTransactions();
      assertNoTxStats();
   }

   public void testPutIfAbsent(Method method) throws InterruptedException {
      final String key1 = k(method, 1);
      final String key2 = k(method, 2);

      assertEmpty(key1, key2);

      put(1, key1, VALUE_1);

      assertCacheValue(key1, VALUE_1);

      //read-only tx
      cache(0).putIfAbsent(key1, VALUE_2);

      assertCacheValue(key1, VALUE_1);

      put(1, key1, VALUE_3);

      assertCacheValue(key1, VALUE_3);

      //read-only tx
      cache(0).putIfAbsent(key1, VALUE_4);

      assertCacheValue(key1, VALUE_3);

      putIfAbsent(0, key2, VALUE_1);

      assertCacheValue(key2, VALUE_1);

      assertNoTransactions();
      assertNoTxStats();
   }

   public void testRemoveIfPresent(Method method) throws InterruptedException {
      final String key1 = k(method, 1);

      assertEmpty(key1);

      put(0, key1, VALUE_1);

      assertCacheValue(key1, VALUE_1);

      put(1, key1, VALUE_2);

      assertCacheValue(key1, VALUE_2);

      //read-only tx
      cache(0).remove(key1, VALUE_1);

      assertCacheValue(key1, VALUE_2);

      remove(0, key1, VALUE_2);

      assertCacheValue(key1, null);

      assertNoTransactions();
      assertNoTxStats();
   }

   public void testClear(Method method) throws InterruptedException {
      final String key1 = k(method, 1);

      assertEmpty(key1);

      put(0, key1, VALUE_1);

      assertCacheValue(key1, VALUE_1);

      cache(0).clear();
      awaitClear(0);

      assertCacheValue(key1, null);

      assertNoTransactions();
      assertNoTxStats();
   }

   public void testReplace(Method method) throws InterruptedException {
      final String key1 = k(method, 1);

      assertEmpty(key1);

      put(1, key1, VALUE_1);

      assertCacheValue(key1, VALUE_1);

      AssertJUnit.assertEquals(replace(0, key1, VALUE_2), VALUE_1);

      assertCacheValue(key1, VALUE_2);

      put(0, key1, VALUE_3);

      assertCacheValue(key1, VALUE_3);

      replace(0, key1, VALUE_3);

      assertCacheValue(key1, VALUE_3);

      put(0, key1, VALUE_4);

      assertCacheValue(key1, VALUE_4);

      assertNoTransactions();
      assertNoTxStats();
   }

   public void testReplaceWithOldVal(Method method) throws InterruptedException {
      final String key1 = k(method, 1);

      assertEmpty(key1);

      put(1, key1, VALUE_1);

      assertCacheValue(key1, VALUE_1);

      put(0, key1, VALUE_2);

      assertCacheValue(key1, VALUE_2);

      //read-only tx
      cache(0).replace(key1, VALUE_3, VALUE_4);

      assertCacheValue(key1, VALUE_2);

      replace(0, key1, VALUE_2, VALUE_4);

      assertCacheValue(key1, VALUE_4);

      assertNoTransactions();
      assertNoTxStats();
   }

   public void testRemoveUnexistingEntry(Method method) throws InterruptedException {
      final String key1 = k(method, 1);

      assertEmpty(key1);

      remove(0, key1);

      assertCacheValue(key1, null);

      assertNoTransactions();
      assertNoTxStats();
   }

   @BeforeMethod(alwaysRun = true)
   public void resetInboundHandler() {
      inboundHandlerList.forEach(ControlledPerCacheInboundInvocationHandler::reset);
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      for (int i = 0; i < 2; ++i) {
         ConfigurationBuilder builder = getDefaultClusteredCacheConfig(mode, true);
         builder.transaction().syncCommitPhase(sync2ndPhase).syncRollbackPhase(sync2ndPhase);
         if (totalOrder) {
            builder.transaction().transactionProtocol(TransactionProtocol.TOTAL_ORDER);
         }
         builder.locking().isolationLevel(IsolationLevel.REPEATABLE_READ).writeSkewCheck(writeSkew);
         builder.clustering().hash().numOwners(1);
         if (writeSkew) {
            builder.versioning().enable().scheme(VersioningScheme.SIMPLE);
         }
         builder.transaction().recovery().disable();
         builder.customInterceptors().addInterceptor().interceptor(new ExtendedStatisticInterceptor())
               .position(InterceptorConfiguration.Position.FIRST);
         addClusterEnabledCacheManager(builder);
      }
      waitForClusterToForm();
      replaceAllPerCacheInboundInvocationHandler();
   }

   protected void assertEmpty(Object... keys) {
      for (Cache cache : caches()) {
         for (Object key : keys) {
            assertNull(cache.get(key));
         }
      }
   }

   protected void assertCacheValue(Object key, Object value) {
      for (int index = 0; index < caches().size(); ++index) {
         if (mode.isSynchronous() && sync2ndPhase) {
            assertEquals(index, key, value);
         } else {
            assertEventuallyEquals(index, key, value);
         }
      }

   }

   protected abstract void awaitPut(int cacheIndex, Object key) throws InterruptedException;

   protected abstract void awaitReplace(int cacheIndex, Object key) throws InterruptedException;

   protected abstract void awaitRemove(int cacheIndex, Object key) throws InterruptedException;

   private void awaitClear(int cacheIndex) throws InterruptedException {
      Set<Address> all = new HashSet<>(cache(cacheIndex).getAdvancedCache().getRpcManager().getMembers());
      all.remove(address(cacheIndex));
      awaitOperation(Operation.CLEAR, all);
   }

   protected abstract void awaitPutMap(int cacheIndex, Collection<Object> keys) throws InterruptedException;

   protected final void awaitOperation(Operation operation, Collection<Address> owners) throws InterruptedException {
      for (int i = 0; i < NUM_NODES; ++i) {
         if (owners.contains(cache(i).getAdvancedCache().getRpcManager().getAddress())) {
            inboundHandlerList.get(i).await(operation, 30, TimeUnit.SECONDS);
         }
      }
   }

   private void put(int cacheIndex, Object key, Object value) throws InterruptedException {
      cache(cacheIndex).put(key, value);
      awaitPut(cacheIndex, key);
   }

   private void putIfAbsent(int cacheIndex, Object key, Object value) throws InterruptedException {
      cache(cacheIndex).putIfAbsent(key, value);
      awaitPut(cacheIndex, key);
   }

   private Object replace(int cacheIndex, Object key, Object value) throws InterruptedException {
      Object val = cache(cacheIndex).replace(key, value);
      awaitReplace(cacheIndex, key);
      return val;
   }

   private Object replace(int cacheIndex, Object key, Object oldValue, Object newValue) throws InterruptedException {
      Object val = cache(cacheIndex).replace(key, oldValue, newValue);
      awaitReplace(cacheIndex, key);
      return val;
   }

   private void remove(int cacheIndex, Object key) throws InterruptedException {
      cache(cacheIndex).remove(key);
      awaitRemove(cacheIndex, key);
   }

   private void remove(int cacheIndex, Object key, Object oldValue) throws InterruptedException {
      cache(cacheIndex).remove(key, oldValue);
      awaitRemove(cacheIndex, key);
   }

   private void assertNoTxStats() {
      final ExtendedStatisticInterceptor[] statisticInterceptors = new ExtendedStatisticInterceptor[caches().size()];
      for (int i = 0; i < caches().size(); ++i) {
         statisticInterceptors[i] = getExtendedStatistic(cache(i));
      }
      eventually(() -> {
         for (ExtendedStatisticInterceptor interceptor : statisticInterceptors) {
            if (interceptor.getCacheStatisticManager().hasPendingTransactions()) {
               return false;
            }
         }
         return true;
      });
   }

   private void assertEquals(int index, Object key, Object value) {
      AssertJUnit.assertEquals(cache(index).get(key), value);
   }

   private ExtendedStatisticInterceptor getExtendedStatistic(Cache<?, ?> cache) {
      SequentialInterceptorChain interceptorChain = cache.getAdvancedCache().getSequentialInterceptorChain();
      ExtendedStatisticInterceptor extendedStatisticInterceptor =
            interceptorChain.findInterceptorExtending(ExtendedStatisticInterceptor.class);
      if (extendedStatisticInterceptor != null) {
         extendedStatisticInterceptor.resetStatistics();
      }
      return extendedStatisticInterceptor;
   }

   private void replaceAllPerCacheInboundInvocationHandler() {
      for (Cache<?, ?> cache : caches()) {
         inboundHandlerList.add(replacePerCacheInboundInvocationHandler(cache));
      }
   }

   private ControlledPerCacheInboundInvocationHandler replacePerCacheInboundInvocationHandler(Cache<?, ?> cache) {
      ControlledPerCacheInboundInvocationHandler handler = new ControlledPerCacheInboundInvocationHandler(
            extractComponent(cache, PerCacheInboundInvocationHandler.class));
      replaceComponent(cache, PerCacheInboundInvocationHandler.class, handler, true);
      replaceField(handler, "inboundInvocationHandler", cache.getAdvancedCache().getComponentRegistry(), ComponentRegistry.class);
      return handler;
   }

   protected enum Operation {
      PUT, REMOVE, REPLACE, CLEAR, PUT_MAP
   }

   private static class ControlledPerCacheInboundInvocationHandler implements PerCacheInboundInvocationHandler {

      private final PerCacheInboundInvocationHandler delegate;
      private final Queue<Operation> operationQueue = new LinkedList<>();

      private ControlledPerCacheInboundInvocationHandler(PerCacheInboundInvocationHandler delegate) {
         this.delegate = delegate;
      }

      @Override
      public void handle(CacheRpcCommand command, Reply reply, DeliverOrder order) {
         checkCommand(command);
         delegate.handle(command, reply, order);
      }

      private void reset() {
         synchronized (operationQueue) {
            operationQueue.clear();
         }
      }

      private void await(Operation operation, long timeout, TimeUnit timeUnit) throws InterruptedException {
         final long timeoutNanos = System.nanoTime() + timeUnit.toNanos(timeout);
         synchronized (operationQueue) {
            while (operationQueue.peek() != operation && System.nanoTime() - timeoutNanos < 0) {
               operationQueue.wait(timeUnit.toMillis(timeout));
            }
            AssertJUnit.assertEquals(operation, operationQueue.poll());
         }
      }

      private void checkCommand(ReplicableCommand cacheRpcCommand) {
         synchronized (operationQueue) {
            switch (cacheRpcCommand.getCommandId()) {
               case PutKeyValueCommand.COMMAND_ID:
                  operationQueue.add(Operation.PUT);
                  break;
               case ReplaceCommand.COMMAND_ID:
                  operationQueue.add(Operation.REPLACE);
                  break;
               case RemoveCommand.COMMAND_ID:
                  operationQueue.add(Operation.REMOVE);
                  break;
               case ClearCommand.COMMAND_ID:
                  operationQueue.add(Operation.CLEAR);
                  break;
               case PutMapCommand.COMMAND_ID:
                  operationQueue.add(Operation.PUT_MAP);
                  break;
               case PrepareCommand.COMMAND_ID:
               case VersionedPrepareCommand.COMMAND_ID:
               case TotalOrderNonVersionedPrepareCommand.COMMAND_ID:
               case TotalOrderVersionedPrepareCommand.COMMAND_ID:
                  for (WriteCommand command : ((PrepareCommand) cacheRpcCommand).getModifications()) {
                     checkCommand(command);
                  }
                  break;
               case SingleRpcCommand.COMMAND_ID:
                  checkCommand(((SingleRpcCommand) cacheRpcCommand).getCommand());
                  break;
            }
            operationQueue.notifyAll();
         }
      }
   }
}
