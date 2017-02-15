package org.infinispan.partitionhandling;

import static org.infinispan.test.TestingUtil.extractComponent;
import static org.infinispan.test.TestingUtil.extractLockManager;
import static org.infinispan.test.TestingUtil.waitForStableTopology;
import static org.infinispan.test.TestingUtil.wrapPerCacheInboundInvocationHandler;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.infinispan.Cache;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.distribution.MagicKey;
import org.infinispan.partitionhandling.impl.PartitionHandlingManager;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.inboundhandler.PerCacheInboundInvocationHandler;
import org.infinispan.remoting.inboundhandler.Reply;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.concurrent.ReclosableLatch;
import org.infinispan.util.concurrent.TimeoutException;
import org.infinispan.util.concurrent.locks.LockManager;
import org.infinispan.util.logging.Log;
import org.testng.AssertJUnit;

/**
 * It tests multiple scenarios where a split can happen during a transaction.
 *
 * @author Pedro Ruivo
 * @since 8.0
 */
public abstract class BaseTxPartitionAndMergeTest extends BasePartitionHandlingTest {

   protected static final String INITIAL_VALUE = "init-value";
   protected static final String FINAL_VALUE = "final-value";

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
      ControlledInboundHandler controlledInboundHandler = wrapPerCacheInboundInvocationHandler(cache, (wrapOn, current) -> new ControlledInboundHandler(current), true);
      controlledInboundHandler.filter = filter;
   }

   protected FilterCollection createFilters(String cacheName, boolean discard, Class<? extends CacheRpcCommand> commandClass, SplitMode splitMode) {
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

   protected void mergeCluster(String cacheName) {
      getLog().debugf("Merging cluster");
      partition(0).merge(partition(1));
      waitForStableTopology(caches(cacheName));
      for (int i = 0; i < numMembersInCluster; i++) {
         PartitionHandlingManager phmI = partitionHandlingManager(cache(i, cacheName));
         eventuallyEquals(AvailabilityMode.AVAILABLE, phmI::getAvailabilityMode);
      }
      getLog().debugf("Cluster merged");
   }

   protected void finalAsserts(String cacheName, KeyInfo keyInfo, String value) {
      assertNoTransactions(cacheName);
      assertNoTransactionsInPartitionHandler(cacheName);
      assertNoLocks(cacheName);

      assertValue(keyInfo.getKey1(), value, this.<Object, String>caches(cacheName));
      assertValue(keyInfo.getKey2(), value, this.<Object, String>caches(cacheName));
   }

   protected void assertNoLocks(String cacheName) {
      eventually("Expected no locks acquired in all nodes.", () -> {
         for (Cache<?, ?> cache : caches(cacheName)) {
            LockManager lockManager = extractLockManager(cache);
            getLog().tracef("Locks info=%s", lockManager.printLockInfo());
            if (lockManager.getNumberOfLocksHeld() != 0) {
               getLog().warnf("Locks acquired on cache '%s'", cache);
               return false;
            }
         }
         return true;
      }, 30000, 500, TimeUnit.MILLISECONDS);
   }

   protected void assertValue(Object key, String value, Collection<Cache<Object, String>> caches) {
      for (Cache<Object, String> cache : caches) {
         AssertJUnit.assertEquals("Wrong value in cache " + address(cache), value, cache.get(key));
      }
   }

   protected KeyInfo createKeys(String cacheName) {
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
            test.getLog().debug("Cluster split.");
         }
      },
      BOTH_DEGRADED {
         @Override
         public void split(BaseTxPartitionAndMergeTest test) {
            test.getLog().debug("Splitting cluster in equal partition");
            test.splitCluster(new int[]{0, 1}, new int[]{2, 3});
            test.getLog().debug("Cluster split.");
         }
      },
      PRIMARY_OWNER_ISOLATED {
         @Override
         public void split(BaseTxPartitionAndMergeTest test) {
            test.getLog().debug("Splitting cluster isolating a primary owner.");
            test.splitCluster(new int[]{2}, new int[]{0, 1, 3});
            test.getLog().debug("Cluster split.");
         }
      };

      public abstract void split(BaseTxPartitionAndMergeTest test);
   }

   private interface AwaitAndUnblock {
      void await(long timeout, TimeUnit timeUnit) throws InterruptedException;

      void unblock();
   }

   private interface Filter extends AwaitAndUnblock {
      boolean before(CacheRpcCommand command, Reply reply, DeliverOrder order);
   }

   private static class ControlledInboundHandler implements PerCacheInboundInvocationHandler {

      private final PerCacheInboundInvocationHandler delegate;
      private volatile Filter filter;

      private ControlledInboundHandler(PerCacheInboundInvocationHandler delegate) {
         this.delegate = delegate;
      }

      @Override
      public void handle(CacheRpcCommand command, Reply reply, DeliverOrder order) {
         final Filter currentFilter = filter;
         if (currentFilter != null && currentFilter.before(command, reply, order)) {
            delegate.handle(command, reply, order);
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
         if (aClass.isAssignableFrom(command.getClass())) {
            notifier.open();
            try {
               blocker.await(30, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
               Thread.currentThread().interrupt();
            }
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
         if (aClass.isAssignableFrom(command.getClass())) {
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

      private DiscardFilter(Class<? extends CacheRpcCommand> aClass) {
         this.aClass = aClass;
         notifier = new ReclosableLatch(false);
      }

      @Override
      public boolean before(CacheRpcCommand command, Reply reply, DeliverOrder order) {
         if (!notifier.isOpened() && aClass.isAssignableFrom(command.getClass())) {
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
   }

   protected static class KeyInfo {
      private final Object key1;
      private final Object key2;

      public KeyInfo(Object key1, Object key2) {
         this.key1 = key1;
         this.key2 = key2;
      }

      public void putFinalValue(Cache<Object, String> cache) {
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

      public FilterCollection(Collection<AwaitAndUnblock> collection) {
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
   }
}
