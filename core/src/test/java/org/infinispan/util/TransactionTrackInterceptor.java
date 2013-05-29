/*
 * JBoss, Home of Professional Open Source
 * Copyright 2013 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */

package org.infinispan.util;

import org.infinispan.Cache;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.InterceptorConfiguration;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.interceptors.InterceptorChain;
import org.infinispan.interceptors.base.BaseCustomInterceptor;
import org.infinispan.transaction.TransactionTable;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * @author Pedro Ruivo
 * @since 5.3
 */
public class TransactionTrackInterceptor extends BaseCustomInterceptor {

   private static final Log log = LogFactory.getLog(TransactionTrackInterceptor.class);
   private final Set<GlobalTransaction> localTransactionsSeen;
   private final Set<GlobalTransaction> remoteTransactionsSeen;
   //ordered local transaction list constructed from the operations.
   private final ArrayList<GlobalTransaction> localTransactionsOperation;

   public TransactionTrackInterceptor() {
      localTransactionsSeen = new HashSet<GlobalTransaction>();
      remoteTransactionsSeen = new HashSet<GlobalTransaction>();
      localTransactionsOperation = new ArrayList<GlobalTransaction>(8);
   }

   public static TransactionTrackInterceptor injectInConfiguration(ConfigurationBuilder builder) {
      TransactionTrackInterceptor interceptor = new TransactionTrackInterceptor();
      builder.customInterceptors().addInterceptor().position(InterceptorConfiguration.Position.FIRST).interceptor(interceptor);
      return interceptor;
   }

   public static TransactionTrackInterceptor injectInCache(Cache<?, ?> cache) {
      InterceptorChain chain = cache.getAdvancedCache().getComponentRegistry().getComponent(InterceptorChain.class);
      if (chain.containsInterceptorType(TransactionTrackInterceptor.class)) {
         return (TransactionTrackInterceptor) chain.getInterceptorsWithClass(TransactionTrackInterceptor.class).get(0);
      }
      TransactionTrackInterceptor interceptor = new TransactionTrackInterceptor();
      //TODO: commented because of ISPN-3066
      //cache.getAdvancedCache().getComponentRegistry().registerComponent(interceptor, TransactionTrackInterceptor.class);
      //TODO: begin of workaround because of ISPN-3066
      interceptor.cache = cache;
      interceptor.embeddedCacheManager = cache.getCacheManager();
      //TODO: end of workaround because of ISPN-3066
      chain.addInterceptor(interceptor, 0);
      return interceptor;
   }

   public synchronized final GlobalTransaction getLastExecutedTransaction() {
      int size = localTransactionsOperation.size();
      if (size == 0) {
         return null;
      }
      return localTransactionsOperation.get(size - 1);
   }

   public synchronized final List<GlobalTransaction> getExecutedTransactions() {
      return Collections.unmodifiableList(localTransactionsOperation);
   }

   @Override
   public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      try {
         return invokeNextInterceptor(ctx, command);
      } finally {
         seen(command.getGlobalTransaction(), ctx.isOriginLocal());
      }
   }

   @Override
   public Object visitRollbackCommand(TxInvocationContext ctx, RollbackCommand command) throws Throwable {
      try {
         return invokeNextInterceptor(ctx, command);
      } finally {
         seen(command.getGlobalTransaction(), ctx.isOriginLocal());
      }
   }

   @Override
   public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
      try {
         return invokeNextInterceptor(ctx, command);
      } finally {
         seen(command.getGlobalTransaction(), ctx.isOriginLocal());
      }
   }

   public boolean awaitForLocalCompletion(GlobalTransaction globalTransaction, long timeout, TimeUnit unit) throws InterruptedException {
      long endTimeout = unit.toMillis(timeout) + System.currentTimeMillis();
      while (System.currentTimeMillis() < endTimeout && !completedLocalTransactions(globalTransaction)) {
         sleep();
      }
      boolean completed = completedLocalTransactions(globalTransaction);
      log.debugf("[local] is %s completed? %s", globalTransaction.getId(), completed);
      return completed;
   }

   public boolean awaitForRemoteCompletion(GlobalTransaction globalTransaction, long timeout, TimeUnit unit) throws InterruptedException {
      long endTimeout = unit.toMillis(timeout) + System.currentTimeMillis();
      while (System.currentTimeMillis() < endTimeout && !completedRemoteTransactions(globalTransaction)) {
         sleep();
      }
      boolean completed = completedRemoteTransactions(globalTransaction);
      log.debugf("[remote] is %s completed? %s", globalTransaction.getId(), completed);
      return completed;
   }

   public boolean awaitForLocalCompletion(int expectedTransactions, long timeout, TimeUnit unit) throws InterruptedException {
      long endTimeout = unit.toMillis(timeout) + System.currentTimeMillis();
      while (System.currentTimeMillis() < endTimeout && completedLocalTransactions() < expectedTransactions) {
         sleep();
      }
      log.debugf("[local] check for completion. seen=%s, expected=%s", localTransactionsSeen.size(), expectedTransactions);
      return completedLocalTransactions() >= expectedTransactions;
   }

   public boolean awaitForRemoteCompletion(int expectedTransactions, long timeout, TimeUnit unit) throws InterruptedException {
      long endTimeout = unit.toMillis(timeout) + System.currentTimeMillis();
      while (System.currentTimeMillis() < endTimeout && completedRemoteTransactions() < expectedTransactions) {
         sleep();
      }
      log.debugf("[remote] check for completion. seen=%s, expected=%s", remoteTransactionsSeen.size(), expectedTransactions);
      return completedRemoteTransactions() >= expectedTransactions;
   }

   public synchronized void reset() {
      localTransactionsSeen.clear();
      remoteTransactionsSeen.clear();
      localTransactionsOperation.clear();
   }

   @Override
   protected Object handleDefault(InvocationContext ctx, VisitableCommand command) throws Throwable {
      try {
         return super.handleDefault(ctx, command);
      } finally {
         if (ctx.isOriginLocal() && ctx.isInTxScope()) {
            GlobalTransaction globalTransaction = ((TxInvocationContext) ctx).getGlobalTransaction();
            synchronized (this) {
               if (!localTransactionsOperation.contains(globalTransaction)) {
                  localTransactionsOperation.add(globalTransaction);
               }
            }
         }
      }
   }

   private synchronized void seen(GlobalTransaction globalTransaction, boolean local) {
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
      return cache.getAdvancedCache().getComponentRegistry().getComponent(TransactionTable.class);
   }
}
