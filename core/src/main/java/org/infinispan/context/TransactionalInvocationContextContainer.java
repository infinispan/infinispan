/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */

package org.infinispan.context;

import org.infinispan.CacheException;
import org.infinispan.context.impl.LocalTxInvocationContext;
import org.infinispan.context.impl.NonTxInvocationContext;
import org.infinispan.context.impl.RemoteTxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.remoting.transport.Address;
import org.infinispan.transaction.LocalTransaction;
import org.infinispan.transaction.RemoteTransaction;
import org.infinispan.transaction.TransactionTable;

import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;

/**
 * Invocation context to be used for transactional caches.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
public class TransactionalInvocationContextContainer extends AbstractInvocationContextContainer {

   private TransactionManager tm;
   private TransactionTable transactionTable;

   @Inject
   public void init(TransactionManager tm, TransactionTable transactionTable) {
      this.tm = tm;
      this.transactionTable = transactionTable;
   }

   @Override
   public NonTxInvocationContext createNonTxInvocationContext() {
      return newNonTxInvocationContext(true);
   }

   public InvocationContext createInvocationContext(boolean isWrite, int keyCount) {
      final Transaction runningTx = getRunningTx();
      if (runningTx == null && !isWrite) {
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