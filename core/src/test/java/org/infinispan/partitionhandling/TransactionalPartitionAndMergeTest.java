package org.infinispan.partitionhandling;

import org.infinispan.Cache;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.control.LockControlCommand;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commons.CacheException;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.distribution.MagicKey;
import org.infinispan.partitionhandling.impl.PartitionHandlingManager;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.inboundhandler.PerCacheInboundInvocationHandler;
import org.infinispan.remoting.inboundhandler.Reply;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.TestingUtil.WrapFactory;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.transaction.lookup.DummyTransactionManagerLookup;
import org.infinispan.transaction.tm.DummyTransaction;
import org.infinispan.transaction.tm.DummyTransactionManager;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.AbstractControlledRpcManager;
import org.infinispan.util.concurrent.ReclosableLatch;
import org.infinispan.util.concurrent.TimeoutException;
import org.infinispan.util.concurrent.locks.LockManager;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

import javax.transaction.RollbackException;
import javax.transaction.Status;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.infinispan.test.TestingUtil.extractComponent;
import static org.infinispan.test.TestingUtil.extractLockManager;
import static org.infinispan.test.TestingUtil.wrapComponent;
import static org.infinispan.test.TestingUtil.wrapPerCacheInboundInvocationHandler;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

/**
 * It tests multiple scenarios where a split can happen during a transaction.
 *
 * @author Pedro Ruivo
 * @since 7.2
 */
@Test(groups = "functional", testName = "partitionhandling.TransactionalPartitionAndMergeTest")
public class TransactionalPartitionAndMergeTest extends BasePartitionHandlingTest {

   private static final String OPTIMISTIC_TX_CACHE_NAME = "opt-cache";
   private static final String PESSIMISTIC_TX_CACHE_NAME = "pes-cache";
   private static final String INITIAL_VALUE = "init-value";
   private static final String FINAL_VALUE = "final-value";
   private static final Log log = LogFactory.getLog(TransactionalPartitionAndMergeTest.class);

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
      ControlledInboundHandler controlledInboundHandler = wrapPerCacheInboundInvocationHandler(cache, new WrapFactory<PerCacheInboundInvocationHandler, ControlledInboundHandler, Cache<?, ?>>() {
         @Override
         public ControlledInboundHandler wrap(Cache<?, ?> wrapOn, PerCacheInboundInvocationHandler current) {
            return new ControlledInboundHandler(current);
         }
      }, true);
      controlledInboundHandler.filter = filter;
   }

   public void testSplitDuringPrepareWithOptimistic() throws Exception {
      doOptimisticTxTest(TransactionPhase.PREPARE, true, true);
   }

   public void testSplitDuringPrepareWithOptimistic2() throws Exception {
      doOptimisticTxTest(TransactionPhase.PREPARE, true, false);
   }

   public void testSplitDuringCommitWithOptimistic() throws Exception {
      doOptimisticTxTest(TransactionPhase.COMMIT, false, true);
   }

   public void testSplitDuringCommitWithOptimistic2() throws Exception {
      doOptimisticTxTest(TransactionPhase.COMMIT, false, false);
   }

   public void testSplitDuringRollbackWithOptimistic() throws Exception {
      doOptimisticTxTest(TransactionPhase.ROLLBACK, true, true);
   }

   public void testSplitDuringRollbackWithOptimistic2() throws Exception {
      doOptimisticTxTest(TransactionPhase.ROLLBACK, true, false);
   }

   public void testSplitBeforeCommitWithOptimistic() throws Exception {
      doTestSplitBeforeWithOptimistic(true);
   }

   public void testSplitBeforeRollbackWithOptimistic() throws Exception {
      doTestSplitBeforeWithOptimistic(false);
   }

   public void testSplitDuringLockWithPessimistic() throws Exception {
      doPessimisticTxTest(TransactionPhase.RUNTIME_LOCK, true, true);
   }

   public void testSplitDuringLockWithPessimistic2() throws Exception {
      doPessimisticTxTest(TransactionPhase.RUNTIME_LOCK, true, false);
   }

   public void testSplitDuringRollbackWithPessimistic() throws Exception {
      doPessimisticTxTest(TransactionPhase.ROLLBACK, true, true);
   }

   public void testSplitDuringRollbackWithPessimistic2() throws Exception {
      doPessimisticTxTest(TransactionPhase.ROLLBACK, true, false);
   }

   public void testSplitDuringCommitWithPessimistic() throws Exception {
      doPessimisticTxTest(TransactionPhase.PREPARE, false, true);
   }

   public void testSplitDuringCommitWithPessimistic2() throws Exception {
      doPessimisticTxTest(TransactionPhase.PREPARE, false, false);
   }

   public void testSplitBeforePrepareWithPessimistic() throws Exception {
      doTestSplitBeforeWithPessimistic(true);
   }

   public void testSplitBeforeRollbackWithPessimistic() throws Exception {
      doTestSplitBeforeWithPessimistic(false);
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      super.createCacheManagers();
      defineConfigurationOnAllManagers(OPTIMISTIC_TX_CACHE_NAME, getConfiguration(LockingMode.OPTIMISTIC));
      defineConfigurationOnAllManagers(PESSIMISTIC_TX_CACHE_NAME, getConfiguration(LockingMode.PESSIMISTIC));
   }

   private void doTestSplitBeforeWithPessimistic(boolean commit) throws Exception {
      //split happens before the commit() or rollback(). Locks are acquired
      waitForClusterToForm(PESSIMISTIC_TX_CACHE_NAME);

      final KeyInfo keyInfo = createKeys(PESSIMISTIC_TX_CACHE_NAME);
      final Cache<Object, String> originator = cache(0, PESSIMISTIC_TX_CACHE_NAME);

      final TransactionManager transactionManager = originator.getAdvancedCache().getTransactionManager();
      transactionManager.begin();
      keyInfo.putFinalValue(originator);
      final Transaction transaction = transactionManager.suspend();

      splitCluster();

      transactionManager.resume(transaction);
      if (commit) {
         transactionManager.commit();
      } else {
         transactionManager.rollback();
      }

      assertLocked(cache(1, PESSIMISTIC_TX_CACHE_NAME), keyInfo.getKey1());
      assertLocked(cache(2, PESSIMISTIC_TX_CACHE_NAME), keyInfo.getKey2());

      mergeCluster();
      finalAsserts(PESSIMISTIC_TX_CACHE_NAME, keyInfo, commit ? FINAL_VALUE : INITIAL_VALUE);
   }

   private void doTestSplitBeforeWithOptimistic(boolean commit) throws Exception {
      //the transaction is successfully prepare and then the split happens before the commit phase starts.
      waitForClusterToForm(OPTIMISTIC_TX_CACHE_NAME);
      final KeyInfo keyInfo = createKeys(OPTIMISTIC_TX_CACHE_NAME);
      final Cache<Object, String> originator = cache(0, OPTIMISTIC_TX_CACHE_NAME);

      final DummyTransactionManager transactionManager = (DummyTransactionManager) originator.getAdvancedCache().getTransactionManager();
      transactionManager.begin();
      final DummyTransaction transaction = transactionManager.getTransaction();
      keyInfo.putFinalValue(originator);
      AssertJUnit.assertTrue(transaction.runPrepare());
      transactionManager.suspend();

      splitCluster();

      transactionManager.resume(transaction);
      transaction.runCommit(!commit);

      //the rollback is not sent. the locks are kept
      assertLocked(cache(1, OPTIMISTIC_TX_CACHE_NAME), keyInfo.getKey1());
      assertLocked(cache(2, OPTIMISTIC_TX_CACHE_NAME), keyInfo.getKey2());

      mergeCluster();
      finalAsserts(OPTIMISTIC_TX_CACHE_NAME, keyInfo, commit ? FINAL_VALUE : INITIAL_VALUE);
   }

   private void doOptimisticTxTest(TransactionPhase phase, boolean txFail, boolean discard) throws Exception {
      waitForClusterToForm(OPTIMISTIC_TX_CACHE_NAME);

      final KeyInfo keyInfo = createKeys(OPTIMISTIC_TX_CACHE_NAME);
      final Cache<Object, String> originator = cache(0, OPTIMISTIC_TX_CACHE_NAME);
      final FilterInfo filterInfo = createFilters(OPTIMISTIC_TX_CACHE_NAME, discard, phase.getCommandClass());

      if (phase == TransactionPhase.ROLLBACK) {
         //make the transaction fail!
         wrapComponent(cache(0, OPTIMISTIC_TX_CACHE_NAME), RpcManager.class, new WrapFactory<RpcManager, RpcManager, Cache<?, ?>>() {
            @Override
            public RpcManager wrap(Cache<?, ?> wrapOn, RpcManager current) {
               return new AbstractControlledRpcManager(current) {
                  @Override
                  protected Map<Address, Response> afterInvokeRemotely(ReplicableCommand command, Map<Address, Response> responseMap) {
                     if (command instanceof PrepareCommand) {
                        throw new CacheException("Induced fail!");
                     }
                     return super.afterInvokeRemotely(command, responseMap);
                  }
               };
            }
         }, true);
      }

      Future<Void> put = fork(new Callable<Void>() {
         @Override
         public Void call() throws Exception {
            final TransactionManager transactionManager = originator.getAdvancedCache().getTransactionManager();
            transactionManager.begin();
            keyInfo.putFinalValue(originator);
            transactionManager.commit();
            return null;
         }
      });

      filterInfo.await();
      splitCluster();
      filterInfo.unblock();

      try {
         put.get();
         assertFalse(txFail);
      } catch (ExecutionException e) {
         assertTrue(txFail);
      }

      switch (phase) {
         case PREPARE:
            //always locked. cache0 is unable to send rollback because it fails with suspected exception.
            assertLocked(cache(1, OPTIMISTIC_TX_CACHE_NAME), keyInfo.getKey1());
            //or the prepare is never received, so key never locked, or it is received and it decides to rollback the transaction.
            assertEventuallyNotLocked(cache(2, OPTIMISTIC_TX_CACHE_NAME), keyInfo.getKey2());
            break;
         case ROLLBACK:
            //key is unlocked because the rollback is always received in cache1
            assertEventuallyNotLocked(cache(1, OPTIMISTIC_TX_CACHE_NAME), keyInfo.getKey1());
            if (discard) {
               //rollback never received, so key is locked until the merge occurs.
               assertLocked(cache(2, OPTIMISTIC_TX_CACHE_NAME), keyInfo.getKey2());
            } else {
               //rollback received, so key is unlocked
               assertEventuallyNotLocked(cache(2, OPTIMISTIC_TX_CACHE_NAME), keyInfo.getKey2());
            }
            break;
         case COMMIT:
            //on both caches, the key is locked and it is unlocked after the merge
            assertLocked(cache(1, OPTIMISTIC_TX_CACHE_NAME), keyInfo.getKey1());
            assertLocked(cache(2, OPTIMISTIC_TX_CACHE_NAME), keyInfo.getKey2());
            break;
      }

      mergeCluster();
      finalAsserts(OPTIMISTIC_TX_CACHE_NAME, keyInfo, txFail ? INITIAL_VALUE : FINAL_VALUE);
   }

   private void doPessimisticTxTest(final TransactionPhase phase, boolean txFail, boolean discard) throws Exception {
      waitForClusterToForm(PESSIMISTIC_TX_CACHE_NAME);

      final KeyInfo keyInfo = createKeys(PESSIMISTIC_TX_CACHE_NAME);
      final Cache<Object, String> originator = cache(0, PESSIMISTIC_TX_CACHE_NAME);
      final FilterInfo filterInfo = createFilters(PESSIMISTIC_TX_CACHE_NAME, discard, phase.getCommandClass());

      Future<Void> put = fork(new Callable<Void>() {
         @Override
         public Void call() throws Exception {
            final TransactionManager transactionManager = originator.getAdvancedCache().getTransactionManager();
            transactionManager.begin();
            final Transaction tx = transactionManager.getTransaction();
            try {
               keyInfo.putFinalValue(originator);
               if (phase == TransactionPhase.ROLLBACK) {
                  tx.setRollbackOnly();
               }
            } finally {
               if (tx.getStatus() == Status.STATUS_MARKED_ROLLBACK) {
                  transactionManager.rollback();
                  //just to make assert in the main thread
                  //noinspection ThrowFromFinallyBlock
                  throw new RollbackException("rollback");
               } else {
                  transactionManager.commit();
               }
            }
            return null;
         }
      });

      filterInfo.await();
      splitCluster();
      filterInfo.unblock();

      try {
         put.get();
         assertFalse(txFail);
      } catch (ExecutionException e) {
         assertTrue(txFail);
      }

      switch (phase) {
         case PREPARE:
            //always locked: locks acquired in runtime
            assertLocked(cache(1, PESSIMISTIC_TX_CACHE_NAME), keyInfo.getKey1());
            if (discard) {
               //locks are acquired during runtime
               assertLocked(cache(2, PESSIMISTIC_TX_CACHE_NAME), keyInfo.getKey2());
            } else {
               //prepare will release the lock
               assertEventuallyNotLocked(cache(2, PESSIMISTIC_TX_CACHE_NAME), keyInfo.getKey2());
            }
            break;
         case ROLLBACK:
            //key is unlocked because the rollback is always received in cache1
            assertEventuallyNotLocked(cache(1, PESSIMISTIC_TX_CACHE_NAME), keyInfo.getKey1());
            if (discard) {
               //rollback never received, so key is locked until the merge occurs.
               assertLocked(cache(2, PESSIMISTIC_TX_CACHE_NAME), keyInfo.getKey2());
            } else {
               //rollback received, so key is unlocked
               assertEventuallyNotLocked(cache(2, PESSIMISTIC_TX_CACHE_NAME), keyInfo.getKey2());
            }
            break;
         case RUNTIME_LOCK:
            //locked rollback is never sent
            assertLocked(cache(1, PESSIMISTIC_TX_CACHE_NAME), keyInfo.getKey1());
            //lock command is discarded since the transaction originator is missing
            assertEventuallyNotLocked(cache(2, PESSIMISTIC_TX_CACHE_NAME), keyInfo.getKey2());
            break;
      }

      mergeCluster();
      finalAsserts(PESSIMISTIC_TX_CACHE_NAME, keyInfo, txFail ? INITIAL_VALUE : FINAL_VALUE);
   }

   private FilterInfo createFilters(String cacheName, boolean discard, Class<? extends CacheRpcCommand> commandClass) {
      final NotifierFilter notifierFilter = notifyCommandOn(cache(1, cacheName), commandClass);
      final DiscardFilter discardFilter = discard ? discardCommandOn(cache(2, cacheName), commandClass) : null;
      final BlockingFilter blockingFilter = !discard ? blockCommandOn(cache(2, cacheName), commandClass) : null;
      return new FilterInfo(notifierFilter, discardFilter, blockingFilter);
   }

   private void splitCluster() {
      log.debugf("Splitting cluster");
      splitCluster(new int[]{0, 1}, new int[]{2, 3});
   }

   private void mergeCluster() {
      log.debugf("Merging cluster");
      partition(0).merge(partition(1));
      log.debugf("Cluster merged");
   }

   private void finalAsserts(String cacheName, KeyInfo keyInfo, String value) {
      assertNoTransactions(cacheName);
      assertNoTransactionsInPartitionHandler(cacheName);
      assertNoLocks(cacheName);

      assertValue(keyInfo.getKey1(), value, this.<Object, String>caches(cacheName));
      assertValue(keyInfo.getKey2(), value, this.<Object, String>caches(cacheName));
   }

   private void assertNoLocks(String cacheName) {
      for (Cache<?, ?> cache : caches(cacheName)) {
         LockManager lockManager = extractLockManager(cache);
         log.tracef("Locks info=%s", lockManager.printLockInfo());
         AssertJUnit.assertEquals(String.format("Locks acquired on cache '%s'", cache), 0, lockManager.getNumberOfLocksHeld());
      }
   }

   private void assertValue(Object key, String value, Collection<Cache<Object, String>> caches) {
      for (Cache<Object, String> cache : caches) {
         AssertJUnit.assertEquals("Wrong value in cache " + address(cache), value, cache.get(key));
      }
   }

   private ConfigurationBuilder getConfiguration(LockingMode lockingMode) {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC);
      builder.clustering().partitionHandling().enabled(true);
      builder.transaction().lockingMode(lockingMode).transactionMode(TransactionMode.TRANSACTIONAL).transactionManagerLookup(new DummyTransactionManagerLookup());
      return builder;
   }

   private void assertNoTransactionsInPartitionHandler(final String cacheName) {
      eventually("Transactions pending in PartitionHandlingManager", new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            for (Cache<?, ?> cache : caches(cacheName)) {
               Collection<GlobalTransaction> partialTransactions = extractComponent(cache, PartitionHandlingManager.class).getPartialTransactions();
               if (!partialTransactions.isEmpty()) {
                  log.debugf("transactions not finished in %s. %s", address(cache), partialTransactions);
                  return false;
               }
            }
            return true;
         }
      });
   }

   private KeyInfo createKeys(String cacheName) {
      final Object key1 = new MagicKey("k1", cache(1, cacheName), cache(2, cacheName));
      final Object key2 = new MagicKey("k2", cache(2, cacheName), cache(1, cacheName));
      cache(1, cacheName).put(key1, INITIAL_VALUE);
      cache(2, cacheName).put(key2, INITIAL_VALUE);
      return new KeyInfo(key1, key2);
   }

   private enum TransactionPhase {
      PREPARE {
         @Override
         Class<? extends CacheRpcCommand> getCommandClass() {
            return PrepareCommand.class;
         }
      },
      COMMIT {
         @Override
         Class<? extends CacheRpcCommand> getCommandClass() {
            return CommitCommand.class;
         }
      },
      ROLLBACK {
         @Override
         Class<? extends CacheRpcCommand> getCommandClass() {
            return RollbackCommand.class;
         }
      },
      RUNTIME_LOCK {
         @Override
         Class<? extends CacheRpcCommand> getCommandClass() {
            return LockControlCommand.class;
         }
      };

      abstract Class<? extends CacheRpcCommand> getCommandClass();
   }

   private interface Filter {
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
               blocker.await();
            } catch (InterruptedException e) {
               Thread.currentThread().interrupt();
            }
         }
         return true;
      }

      public void awaitUntilBlocked(long timeout, TimeUnit timeUnit) throws InterruptedException {
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

      public void awaitUntilDiscarded(long timeout, TimeUnit timeUnit) throws InterruptedException {
         if (!notifier.await(timeout, timeUnit)) {
            throw new TimeoutException();
         }
      }
   }

   private static class KeyInfo {
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

   private static class FilterInfo {
      private final NotifierFilter notifierFilter;
      private final DiscardFilter discardFilter;
      private final BlockingFilter blockingFilter;

      public FilterInfo(NotifierFilter notifierFilter, DiscardFilter discardFilter, BlockingFilter blockingFilter) {
         this.notifierFilter = notifierFilter;
         this.discardFilter = discardFilter;
         this.blockingFilter = blockingFilter;
      }

      public void await() throws InterruptedException {
         if (notifierFilter != null) {
            notifierFilter.await(30, TimeUnit.SECONDS);
         }
         if (discardFilter != null) {
            discardFilter.awaitUntilDiscarded(30, TimeUnit.SECONDS);
         }
         if (blockingFilter != null) {
            blockingFilter.awaitUntilBlocked(30, TimeUnit.SECONDS);
         }
      }

      public void unblock() {
         if (blockingFilter != null) {
            blockingFilter.unblock();
         }
      }
   }
}
