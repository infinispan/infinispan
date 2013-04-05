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
import org.infinispan.configuration.cache.ClusterCacheLoaderConfiguration;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.CacheLoaderConfiguration;
import org.infinispan.context.impl.LocalTxInvocationContext;
import org.infinispan.context.impl.NonTxInvocationContext;
import org.infinispan.context.impl.RemoteTxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
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
   private boolean isThreadLocalRequired;

   @Inject
   public void init(TransactionManager tm,
         TransactionTable transactionTable, Configuration config) {
      super.init(config);
      this.tm = tm;
      this.transactionTable = transactionTable;
   }

   @Start
   public void start() {
      super.start();
      isThreadLocalRequired =
            config.clustering().cacheMode().isClustered()
                  || config.storeAsBinary().enabled()
                  || hasClusterCacheLoader();
   }

   private boolean hasClusterCacheLoader() {
      boolean hasCacheLoaders = config.loaders().usingCacheLoaders();
      if (hasCacheLoaders) {
         List<CacheLoaderConfiguration> loaderConfigs = config.loaders().cacheLoaders();
         for (CacheLoaderConfiguration loaderConfig : loaderConfigs) {
            if (loaderConfig instanceof ClusterCacheLoaderConfiguration)
               return true;
         }
      }
      return false;
   }

   @Override
   public NonTxInvocationContext createNonTxInvocationContext() {
      return newNonTxInvocationContext(true);
   }

   @Override
   public InvocationContext createSingleKeyNonTxInvocationContext() {
      InvocationContext ctx = new SingleKeyNonTxInvocationContext(true, keyEq);

      // Required only for marshaller is required, or cluster cache loader needed
      if (isThreadLocalRequired)
         ctxHolder.set(ctx);

      return ctx;
   }

   @Override
   public InvocationContext createInvocationContext(
         boolean isWrite, int keyCount) {
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
      if (tx == null) throw new IllegalArgumentException("Cannot create a transactional context without a valid Transaction instance.");
      LocalTxInvocationContext localContext = new LocalTxInvocationContext(keyEq);
      LocalTransaction localTransaction = transactionTable.getLocalTransaction(tx);
      localContext.setLocalTransaction(localTransaction);
      localContext.setTransaction(tx);
      ctxHolder.set(localContext);
      return localContext;
   }

   @Override
   public LocalTxInvocationContext createTxInvocationContext() {
      LocalTxInvocationContext ctx = new LocalTxInvocationContext(keyEq);
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

   private Transaction getRunningTx() {
      try {
         return tm.getTransaction();
      } catch (SystemException e) {
         throw new CacheException(e);
      }
   }

   protected final NonTxInvocationContext newNonTxInvocationContext(boolean local) {
      NonTxInvocationContext ctx = new NonTxInvocationContext(keyEq);
      ctx.setOriginLocal(local);
      ctxHolder.set(ctx);
      return ctx;
   }
}