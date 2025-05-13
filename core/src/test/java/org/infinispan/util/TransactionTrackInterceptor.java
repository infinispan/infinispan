package org.infinispan.util;

import static org.infinispan.test.TestingUtil.extractInterceptorChain;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.infinispan.Cache;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.interceptors.AsyncInterceptorChain;
import org.infinispan.interceptors.BaseCustomAsyncInterceptor;
import org.infinispan.test.TestingUtil;
import org.infinispan.transaction.impl.TransactionTable;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * @author Pedro Ruivo
 * @since 5.3
 */
public class TransactionTrackInterceptor extends BaseCustomAsyncInterceptor {

   private static final Log log = LogFactory.getLog(TransactionTrackInterceptor.class);
   private static final GlobalTransaction CLEAR_TRANSACTION = new ClearGlobalTransaction();

   private final Set<GlobalTransaction> localTransactionsSeen;
   private final Set<GlobalTransaction> remoteTransactionsSeen;
   //ordered local transaction list constructed from the operations.
   private final ArrayList<GlobalTransaction> localTransactionsOperation;

   private TransactionTrackInterceptor() {
      localTransactionsSeen = new HashSet<>();
      remoteTransactionsSeen = new HashSet<>();
      localTransactionsOperation = new ArrayList<>(8);
   }

   public static TransactionTrackInterceptor injectInCache(Cache<?, ?> cache) {
      AsyncInterceptorChain chain = extractInterceptorChain(cache);
      if (chain.containsInterceptorType(TransactionTrackInterceptor.class)) {
         return chain.findInterceptorWithClass(TransactionTrackInterceptor.class);
      }
      TransactionTrackInterceptor interceptor = new TransactionTrackInterceptor();
      ComponentRegistry.of(cache).wireDependencies(interceptor);
      TestingUtil.startComponent(interceptor);
      chain.addInterceptor(interceptor, 0);
      return interceptor;
   }

   public final synchronized GlobalTransaction getLastExecutedTransaction() {
      int size = localTransactionsOperation.size();
      if (size == 0) {
         return null;
      }
      return localTransactionsOperation.get(size - 1);
   }

   public final synchronized List<GlobalTransaction> getExecutedTransactions() {
      return Collections.unmodifiableList(localTransactionsOperation);
   }

   @Override
   public Object visitClearCommand(InvocationContext ctx, ClearCommand command) throws Throwable {
      return invokeNextAndFinally(ctx, command, (rCtx, rCommand, rv, t) -> {
         if (rCtx.isOriginLocal()) {
            addLocalTransaction(CLEAR_TRANSACTION);
            //in total order, the transactions are self delivered. So, we simulate the self-deliver of the clear command.
            seen(CLEAR_TRANSACTION, false);
         }
         seen(CLEAR_TRANSACTION, rCtx.isOriginLocal());
      });
   }

   @Override
   public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      return invokeNextAndFinally(ctx, command, (rCtx, rCommand, rv, t) -> {
         seen(command.getGlobalTransaction(), rCtx.isOriginLocal());
      });
   }

   @Override
   public Object visitRollbackCommand(TxInvocationContext ctx, RollbackCommand command) throws Throwable {
      return invokeNextAndFinally(ctx, command, (rCtx, rCommand, rv, t) -> {
         seen(command.getGlobalTransaction(), rCtx.isOriginLocal());
      });
   }

   @Override
   public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
      return invokeNextAndFinally(ctx, command, (rCtx, rCommand, rv, t) -> {
         seen(command.getGlobalTransaction(), rCtx.isOriginLocal());
      });
   }

   public boolean awaitForLocalCompletion(GlobalTransaction globalTransaction, long timeout, TimeUnit unit) throws InterruptedException {
      long endTimeout = unit.toMillis(timeout) + System.currentTimeMillis();
      while (System.currentTimeMillis() < endTimeout && !completedLocalTransactions(globalTransaction)) {
         sleep();
      }
      boolean completed = completedLocalTransactions(globalTransaction);
      if (log.isDebugEnabled()) {
         log.debugf("[local] is %d completed? %s", (Object)globalTransaction.getId(), completed);
      }
      return completed;
   }

   public boolean awaitForRemoteCompletion(GlobalTransaction globalTransaction, long timeout, TimeUnit unit) throws InterruptedException {
      long endTimeout = unit.toMillis(timeout) + System.currentTimeMillis();
      while (System.currentTimeMillis() < endTimeout && !completedRemoteTransactions(globalTransaction)) {
         sleep();
      }
      boolean completed = completedRemoteTransactions(globalTransaction);
      if (log.isDebugEnabled()) {
         log.debugf("[remote] is %d completed? %s", (Object)globalTransaction.getId(), completed);
      }
      return completed;
   }

   public boolean awaitForLocalCompletion(int expectedTransactions, long timeout, TimeUnit unit) throws InterruptedException {
      long endTimeout = unit.toMillis(timeout) + System.currentTimeMillis();
      while (System.currentTimeMillis() < endTimeout && completedLocalTransactions() < expectedTransactions) {
         sleep();
      }
      if (log.isDebugEnabled()) {
         log.debugf("[local] check for completion. seen=%s, expected=%s", localTransactionsSeen.size(), expectedTransactions);
      }
      return completedLocalTransactions() >= expectedTransactions;
   }

   public boolean awaitForRemoteCompletion(int expectedTransactions, long timeout, TimeUnit unit) throws InterruptedException {
      long endTimeout = unit.toMillis(timeout) + System.currentTimeMillis();
      while (System.currentTimeMillis() < endTimeout && completedRemoteTransactions() < expectedTransactions) {
         sleep();
      }
      if (log.isDebugEnabled()) {
         log.debugf("[remote] check for completion. seen=%s, expected=%s", remoteTransactionsSeen.size(), expectedTransactions);
      }
      return completedRemoteTransactions() >= expectedTransactions;
   }

   public synchronized void reset() {
      localTransactionsSeen.clear();
      remoteTransactionsSeen.clear();
      localTransactionsOperation.clear();
   }

   @Override
   protected Object handleDefault(InvocationContext ctx, VisitableCommand command) throws Throwable {
      return invokeNextAndFinally(ctx, command, (rCtx, rCommand, rv, t) -> {
         if (rCtx.isOriginLocal() && rCtx.isInTxScope()) {
            GlobalTransaction globalTransaction = ((TxInvocationContext) rCtx).getGlobalTransaction();
            addLocalTransaction(globalTransaction);
         }
      });
   }

   private synchronized void addLocalTransaction(GlobalTransaction globalTransaction) {
      if (!localTransactionsOperation.contains(globalTransaction)) {
         localTransactionsOperation.add(globalTransaction);
      }
   }

   private synchronized void seen(GlobalTransaction globalTransaction, boolean local) {
      log.tracef("Seen transaction %s. Local? %s", globalTransaction, local);
      if (local) {
         localTransactionsSeen.add(globalTransaction);
      } else {
         remoteTransactionsSeen.add(globalTransaction);
      }
   }

   private void sleep() throws InterruptedException {
      Thread.sleep(100);
   }

   private synchronized int completedLocalTransactions() {
      int count = 0;
      TransactionTable transactionTable = getTransactionTable();
      for (GlobalTransaction tx : localTransactionsSeen) {
         if (!transactionTable.containsLocalTx(tx)) {
            count++;
         }
      }
      return count;
   }

   private synchronized int completedRemoteTransactions() {
      int count = 0;
      TransactionTable transactionTable = getTransactionTable();
      for (GlobalTransaction tx : remoteTransactionsSeen) {
         if (!transactionTable.containRemoteTx(tx)) {
            count++;
         }
      }
      return count;
   }

   private synchronized boolean completedLocalTransactions(GlobalTransaction globalTransaction) {
      return localTransactionsSeen.contains(globalTransaction) &&
            !getTransactionTable().containsLocalTx(globalTransaction);
   }

   private synchronized boolean completedRemoteTransactions(GlobalTransaction globalTransaction) {
      return remoteTransactionsSeen.contains(globalTransaction) &&
            !getTransactionTable().containRemoteTx(globalTransaction);
   }

   private TransactionTable getTransactionTable() {
      return ComponentRegistry.componentOf(cache, TransactionTable.class);
   }

   private static class ClearGlobalTransaction extends GlobalTransaction {
      ClearGlobalTransaction() {
         super(null, false);
      }
   }
}
