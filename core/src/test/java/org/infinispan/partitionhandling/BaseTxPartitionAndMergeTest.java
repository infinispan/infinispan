package org.infinispan.partitionhandling;

import static org.infinispan.test.TestingUtil.extractComponent;
import static org.infinispan.test.TestingUtil.extractLockManager;
import static org.infinispan.test.TestingUtil.waitForNoRebalance;
import static org.infinispan.test.TestingUtil.wrapInboundInvocationHandler;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.infinispan.Cache;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.commons.TimeoutException;
import org.infinispan.distribution.MagicKey;
import org.infinispan.partitionhandling.impl.PartitionHandlingManager;
import org.infinispan.remoting.inboundhandler.AbstractDelegatingHandler;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.inboundhandler.PerCacheInboundInvocationHandler;
import org.infinispan.remoting.inboundhandler.Reply;
import org.infinispan.test.TestingUtil;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.concurrent.ReclosableLatch;
import org.infinispan.util.concurrent.locks.LockManager;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.AssertJUnit;

/**
 * It tests multiple scenarios where a split can happen during a transaction.
 *
 * @author Pedro Ruivo
 * @since 8.0
 */
public abstract class BaseTxPartitionAndMergeTest extends BasePartitionHandlingTest {
   private static final Log log = LogFactory.getLog(BaseTxPartitionAndMergeTest.class);

   static final String INITIAL_VALUE = "init-value";
   static final String FINAL_VALUE = "final-value";

   private static NotifierFilter notifyCommandOn(Cache<?, ?> cache, Class<? extends CacheRpcCommand> blockClass) {
      NotifierFilter filter = new NotifierFilter(blockClass);
      wrapAndApplyFilter(cache, filter);
      return filter;
   }

   private static BlockingFilter blockCommandOn(Cache<?, ?> cache, Class<? extends CacheRpcCommand> blockClass) {
      BlockingFilter filter = new BlockingFilter(blockClass);
      wrapAndApplyFilter(cache, filter);
      return filter;
   }

   private static DiscardFilter discardCommandOn(Cache<?, ?> cache, Class<? extends CacheRpcCommand> blockClass) {
      DiscardFilter filter = new DiscardFilter(blockClass);
      wrapAndApplyFilter(cache, filter);
      return filter;
   }

   private static void wrapAndApplyFilter(Cache<?, ?> cache, Filter filter) {
      ControlledInboundHandler controlledInboundHandler =
         wrapInboundInvocationHandler(cache, delegate -> new ControlledInboundHandler(delegate, filter));
   }

   FilterCollection createFilters(String cacheName, boolean discard, Class<? extends CacheRpcCommand> commandClass,
         SplitMode splitMode) {
      Collection<AwaitAndUnblock> collection = new ArrayList<>(2);
      if (splitMode == SplitMode.ORIGINATOR_ISOLATED) {
         if (discard) {
            collection.add(discardCommandOn(cache(1, cacheName), commandClass));
            collection.add(discardCommandOn(cache(2, cacheName), commandClass));
         } else {
            collection.add(blockCommandOn(cache(1, cacheName), commandClass));
            collection.add(blockCommandOn(cache(2, cacheName), commandClass));
         }
      } else {
         collection.add(notifyCommandOn(cache(1, cacheName), commandClass));
         if (discard) {
            collection.add(discardCommandOn(cache(2, cacheName), commandClass));
         } else {
            collection.add(blockCommandOn(cache(2, cacheName), commandClass));
         }
      }
      return new FilterCollection(collection);
   }

   protected abstract Log getLog();

   void mergeCluster(String cacheName) {
      getLog().debugf("Merging cluster");
      partition(0).merge(partition(1));
      waitForNoRebalance(caches(cacheName));
      for (int i = 0; i < numMembersInCluster; i++) {
         PartitionHandlingManager phmI = partitionHandlingManager(cache(i, cacheName));
         eventuallyEquals(AvailabilityMode.AVAILABLE, phmI::getAvailabilityMode);
      }
      getLog().debugf("Cluster merged");
   }

   void finalAsserts(String cacheName, KeyInfo keyInfo, String value) {
      assertNoTransactions(cacheName);
      assertNoTransactionsInPartitionHandler(cacheName);
      assertNoLocks(cacheName);

      assertValue(keyInfo.getKey1(), value, this.caches(cacheName));
      assertValue(keyInfo.getKey2(), value, this.caches(cacheName));
   }

   protected void assertNoLocks(String cacheName) {
      eventually("Expected no locks acquired in all nodes", () -> {
         for (Cache<?, ?> cache : caches(cacheName)) {
            LockManager lockManager = extractLockManager(cache);
            getLog().tracef("Locks info=%s", lockManager.printLockInfo());
            if (lockManager.getNumberOfLocksHeld() != 0) {
               getLog().warnf("Locks acquired on cache '%s'", cache);
               return false;
            }
         }
         return true;
      }, 30000, TimeUnit.MILLISECONDS);
   }

   protected void assertValue(Object key, String value, Collection<Cache<Object, String>> caches) {
      for (Cache<Object, String> cache : caches) {
         AssertJUnit.assertEquals("Wrong value in cache " + address(cache), value, cache.get(key));
      }
   }

   KeyInfo createKeys(String cacheName) {
      final Object key1 = new MagicKey("k1", cache(1, cacheName), cache(2, cacheName));
      final Object key2 = new MagicKey("k2", cache(2, cacheName), cache(1, cacheName));
      cache(1, cacheName).put(key1, INITIAL_VALUE);
      cache(2, cacheName).put(key2, INITIAL_VALUE);
      return new KeyInfo(key1, key2);
   }

   private void assertNoTransactionsInPartitionHandler(final String cacheName) {
      eventually("Transactions pending in PartitionHandlingManager", () -> {
         for (Cache<?, ?> cache : caches(cacheName)) {
            Collection<GlobalTransaction> partialTransactions = extractComponent(cache, PartitionHandlingManager.class).getPartialTransactions();
            if (!partialTransactions.isEmpty()) {
               getLog().debugf("transactions not finished in %s. %s", address(cache), partialTransactions);
               return false;
            }
         }
         return true;
      });
   }

   protected enum SplitMode {
      ORIGINATOR_ISOLATED {
         @Override
         public void split(BaseTxPartitionAndMergeTest test) {
            test.getLog().debug("Splitting cluster isolating the originator.");
            test.splitCluster(new int[]{0}, new int[]{1, 2, 3});
            assertDegradedPartition(test, 0);
            TestingUtil.waitForNoRebalance(test.cache(1), test.cache(2), test.cache(3));
            test.getLog().debug("Cluster split.");
         }
      },
      BOTH_DEGRADED {
         @Override
         public void split(BaseTxPartitionAndMergeTest test) {
            test.getLog().debug("Splitting cluster in equal partition");
            test.splitCluster(new int[]{0, 1}, new int[]{2, 3});
            assertDegradedPartition(test, 0, 1);
            test.getLog().debug("Cluster split.");
         }
      },
      PRIMARY_OWNER_ISOLATED {
         @Override
         public void split(BaseTxPartitionAndMergeTest test) {
            test.getLog().debug("Splitting cluster isolating a primary owner.");
            test.splitCluster(new int[]{2}, new int[]{0, 1, 3});
            assertDegradedPartition(test, 0);
            TestingUtil.waitForNoRebalance(test.cache(0), test.cache(1), test.cache(3));
            test.getLog().debug("Cluster split.");
         }
      };

      public abstract void split(BaseTxPartitionAndMergeTest test);

      private static void assertDegradedPartition(BaseTxPartitionAndMergeTest test, int... partitionIndexes) {
         for (int i = 0; i < partitionIndexes.length; i++)
            test.partition(i).assertDegradedMode();
      }
   }

   private interface AwaitAndUnblock {
      void await(long timeout, TimeUnit timeUnit) throws InterruptedException;

      void unblock();
   }

   private interface Filter extends AwaitAndUnblock {
      boolean before(CacheRpcCommand command, Reply reply, DeliverOrder order);
   }

   private static class ControlledInboundHandler extends AbstractDelegatingHandler {
      private final Filter filter;

      private ControlledInboundHandler(PerCacheInboundInvocationHandler delegate, Filter filter) {
         super(delegate);
         this.filter = filter;
      }

      @Override
      public void handle(CacheRpcCommand command, Reply reply, DeliverOrder order) {
         final Filter currentFilter = filter;
         if (currentFilter != null && currentFilter.before(command, reply, order)) {
            delegate.handle(command, reply, order);
         } else {
            log.debugf("Ignoring command %s", command);
         }
      }
   }

   private static class BlockingFilter implements Filter {

      private final Class<? extends CacheRpcCommand> aClass;
      private final ReclosableLatch notifier;
      private final ReclosableLatch blocker;

      private BlockingFilter(Class<? extends CacheRpcCommand> aClass) {
         this.aClass = aClass;
         blocker = new ReclosableLatch(false);
         notifier = new ReclosableLatch(false);
      }

      @Override
      public boolean before(CacheRpcCommand command, Reply reply, DeliverOrder order) {
         log.tracef("[Blocking] Checking command %s.", command);
         if (aClass.isAssignableFrom(command.getClass())) {
            log.tracef("[Blocking] Blocking command %s", command);
            notifier.open();
            try {
               blocker.await(30, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
               Thread.currentThread().interrupt();
            }
            log.tracef("[Blocking] Unblocking command %s", command);
         }
         return true;
      }

      public void await(long timeout, TimeUnit timeUnit) throws InterruptedException {
         if (!notifier.await(timeout, timeUnit)) {
            throw new TimeoutException();
         }
      }

      public void unblock() {
         blocker.open();
      }
   }

   private static class NotifierFilter implements Filter {

      private final Class<? extends CacheRpcCommand> aClass;
      private final CountDownLatch notifier;

      private NotifierFilter(Class<? extends CacheRpcCommand> aClass) {
         this.aClass = aClass;
         notifier = new CountDownLatch(1);
      }

      @Override
      public boolean before(CacheRpcCommand command, Reply reply, DeliverOrder order) {
         log.tracef("[Notifier] Checking command %s.", command);
         if (aClass.isAssignableFrom(command.getClass())) {
            log.tracef("[Notifier] Notifying command %s.", command);
            notifier.countDown();
         }
         return true;
      }

      public void await(long timeout, TimeUnit timeUnit) throws InterruptedException {
         if (!notifier.await(timeout, timeUnit)) {
            throw new TimeoutException();
         }
      }

      @Override
      public void unblock() {
         /*no-op*/
      }
   }

   private static class DiscardFilter implements Filter {

      private final Class<? extends CacheRpcCommand> aClass;
      private final ReclosableLatch notifier;
      private volatile boolean discard;

      private DiscardFilter(Class<? extends CacheRpcCommand> aClass) {
         this.aClass = aClass;
         notifier = new ReclosableLatch(false);
         discard = true; //discard everything by default. change if needed in the future.
      }

      @Override
      public boolean before(CacheRpcCommand command, Reply reply, DeliverOrder order) {
         log.tracef("[Discard] Checking command %s. (discard enabled=%s)", command, discard);
         if (discard && aClass.isAssignableFrom(command.getClass())) {
            log.tracef("[Discard] Discarding command %s.", command);
            notifier.open();
            return false;
         }
         return true;
      }

      public void await(long timeout, TimeUnit timeUnit) throws InterruptedException {
         if (!notifier.await(timeout, timeUnit)) {
            throw new TimeoutException();
         }
      }

      @Override
      public void unblock() {
         /*no-op*/
      }

      private void stopDiscard() {
         discard = false;
      }
   }

   protected static class KeyInfo {
      private final Object key1;
      private final Object key2;

      KeyInfo(Object key1, Object key2) {
         this.key1 = key1;
         this.key2 = key2;
      }

      void putFinalValue(Cache<Object, String> cache) {
         cache.put(key1, FINAL_VALUE);
         cache.put(key2, FINAL_VALUE);
      }

      public Object getKey1() {
         return key1;
      }

      public Object getKey2() {
         return key2;
      }
   }

   protected static class FilterCollection implements AwaitAndUnblock {
      private final Collection<AwaitAndUnblock> collection;

      FilterCollection(Collection<AwaitAndUnblock> collection) {
         this.collection = collection;
      }

      @Override
      public void await(long timeout, TimeUnit timeUnit) throws InterruptedException {
         for (AwaitAndUnblock await : collection) {
            await.await(timeout, timeUnit);
         }
      }

      public void unblock() {
         collection.forEach(BaseTxPartitionAndMergeTest.AwaitAndUnblock::unblock);
      }

      public void stopDiscard() {
         collection.stream()
               .filter(DiscardFilter.class::isInstance)
               .map(DiscardFilter.class::cast)
               .forEach(DiscardFilter::stopDiscard);
      }
   }
}
