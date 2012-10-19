/*
 * Copyright 2012 Red Hat, Inc. and/or its affiliates.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA
 */

package org.infinispan.context;

import org.infinispan.CacheException;
import org.infinispan.config.Configuration;
import org.infinispan.context.impl.LocalTxInvocationContext;
import org.infinispan.context.impl.NonTxInvocationContext;
import org.infinispan.context.impl.RemoteTxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.loaders.CacheLoaderConfig;
import org.infinispan.loaders.cluster.ClusterCacheLoaderConfig;
import org.infinispan.remoting.transport.Address;
import org.infinispan.transaction.LocalTransaction;
import org.infinispan.transaction.RemoteTransaction;
import org.infinispan.transaction.TransactionTable;

import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import java.util.List;

/**
 * Invocation context to be used for transactional caches.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
public class TransactionalInvocationContextContainer extends AbstractInvocationContextContainer {

   private TransactionManager tm;
   private TransactionTable transactionTable;
   private Configuration config;
   private boolean isThreadLocalRequired;

   @Inject
   public void init(TransactionManager tm,
         TransactionTable transactionTable, Configuration config) {
      this.tm = tm;
      this.transactionTable = transactionTable;
      this.config = config;
   }

   @Start
   public void start() {
      isThreadLocalRequired =
            config.getCacheMode().isClustered()
                  || config.isStoreAsBinary()
                  || hasClusterCacheLoader();
   }

   private boolean hasClusterCacheLoader() {
      List<CacheLoaderConfig> cacheLoaders = config.getCacheLoaders();
      for (CacheLoaderConfig loaderConfig : cacheLoaders) {
         if (loaderConfig instanceof ClusterCacheLoaderConfig)
            return true;
      }
      return false;
   }

   @Override
   public NonTxInvocationContext createNonTxInvocationContext() {
      return newNonTxInvocationContext(true);
   }

   @Override
   public InvocationContext createSingleKeyNonTxInvocationContext() {
      InvocationContext ctx = new SingleKeyNonTxInvocationContext(true);

      // Required only for marshaller is required, or cluster cache loader needed
      if (isThreadLocalRequired)
         ctxHolder.set(ctx);

      return ctx;
   }

   @Override
   public InvocationContext createInvocationContext(boolean isWrite, int keyCount) {
      final Transaction runningTx = getRunningTx();
      if (runningTx == null && !isWrite) {
         if (keyCount == 1)
            return createSingleKeyNonTxInvocationContext();
         else
            return newNonTxInvocationContext(true);
      }
      return createInvocationContext(runningTx);
   }

   @Override
   public InvocationContext createInvocationContext(Transaction tx) {
      if (tx == null) throw new IllegalStateException("This is a tx cache!");
      LocalTxInvocationContext localContext = new LocalTxInvocationContext();
      LocalTransaction localTransaction = transactionTable.getLocalTransaction(tx);
      localContext.setLocalTransaction(localTransaction);
      localContext.setTransaction(tx);
      ctxHolder.set(localContext);
      return localContext;
   }

   @Override
   public LocalTxInvocationContext createTxInvocationContext() {
      LocalTxInvocationContext ctx = new LocalTxInvocationContext();
      ctxHolder.set(ctx);
      return ctx;
   }

   @Override
   public RemoteTxInvocationContext createRemoteTxInvocationContext(
         RemoteTransaction tx, Address origin) {
      RemoteTxInvocationContext ctx = new RemoteTxInvocationContext();
      ctx.setOrigin(origin);
      ctx.setRemoteTransaction(tx);
      ctxHolder.set(ctx);
      return ctx;
   }

   @Override
   public NonTxInvocationContext createRemoteInvocationContext(Address origin) {
      final NonTxInvocationContext nonTxInvocationContext = newNonTxInvocationContext(false);
      nonTxInvocationContext.setOrigin(origin);
      return nonTxInvocationContext;
   }

   public InvocationContext getInvocationContext() {
      InvocationContext invocationContext = this.ctxHolder.get();
      if (invocationContext == null)
         throw new IllegalStateException("This method can only be called after associating the current thread with a context");
      return invocationContext;
   }

   private Transaction getRunningTx() {
      try {
         return tm.getTransaction();
      } catch (SystemException e) {
         throw new CacheException(e);
      }
   }

   protected final NonTxInvocationContext newNonTxInvocationContext(boolean local) {
      NonTxInvocationContext ctx = new NonTxInvocationContext();
      ctx.setOriginLocal(local);
      ctxHolder.set(ctx);
      return ctx;
   }
}