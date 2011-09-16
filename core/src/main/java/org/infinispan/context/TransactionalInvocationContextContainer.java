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
import org.infinispan.batch.BatchContainer;
import org.infinispan.context.impl.LocalTxInvocationContext;
import org.infinispan.context.impl.NonTxInvocationContext;
import org.infinispan.context.impl.RemoteTxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.remoting.transport.Address;
import org.infinispan.transaction.LocalTransaction;
import org.infinispan.transaction.TransactionTable;

import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;

/**
 * Default implementation for {@link InvocationContextContainer}.
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
public class TransactionalInvocationContextContainer extends AbstractInvocationContextContainer {

   private TransactionManager tm;
   private TransactionTable transactionTable;
   private BatchContainer batchContainer;

   @Inject
   public void init(TransactionManager tm, TransactionTable transactionTable, BatchContainer batchContainer) {
      this.tm = tm;
      this.transactionTable = transactionTable;
      this.batchContainer = batchContainer;
   }

   @Override
   public NonTxInvocationContext createNonTxInvocationContext() {
      return newNonTxInvocationContext(true);
   }

   public InvocationContext createInvocationContext(boolean isWrite) {
      final Transaction runningTx = getRunningTx();
      if (runningTx == null && !isWrite) {
         return newNonTxInvocationContext(true);
      }
      return createInvocationContext(runningTx);
   }

   @Override
   public InvocationContext createInvocationContext(Transaction tx) {
      InvocationContext existing = icTl.get();
      if (tx != null) {
         LocalTxInvocationContext localContext;
         if ((existing == null) || !(existing instanceof LocalTxInvocationContext)) {
            localContext = new LocalTxInvocationContext();
            icTl.set(localContext);
         } else {
            localContext = (LocalTxInvocationContext) existing;
         }
         LocalTransaction localTransaction = transactionTable.getLocalTransaction(tx);
         localContext.setLocalTransaction(localTransaction);
         localContext.setTransaction(tx);
         return localContext;
      } else {
         throw new IllegalStateException("This is a tx cache!");
      }
   }

   public LocalTxInvocationContext createTxInvocationContext() {
      InvocationContext existing = icTl.get();
      if (existing != null && existing instanceof LocalTxInvocationContext) {
         return (LocalTxInvocationContext) existing;
      }
      LocalTxInvocationContext localTxContext = new LocalTxInvocationContext();
      icTl.set(localTxContext);
      return localTxContext;
   }

   public RemoteTxInvocationContext createRemoteTxInvocationContext(Address origin) {
      InvocationContext existing = icTl.get();
      if (existing != null && existing instanceof RemoteTxInvocationContext) {
         return (RemoteTxInvocationContext) existing;
      }
      RemoteTxInvocationContext remoteTxContext = new RemoteTxInvocationContext();
      icTl.set(remoteTxContext);
      remoteTxContext.setOrigin(origin);
      return remoteTxContext;
   }

   public NonTxInvocationContext createRemoteInvocationContext(Address origin) {
      final NonTxInvocationContext nonTxInvocationContext = newNonTxInvocationContext(false);
      nonTxInvocationContext.setOrigin(origin);
      return nonTxInvocationContext;
   }

   private NonTxInvocationContext newNonTxInvocationContext(boolean local) {
      final InvocationContext existing = icTl.get();
      NonTxInvocationContext ctx;
      if (existing != null && existing instanceof NonTxInvocationContext) {
         ctx = (NonTxInvocationContext) existing;
      } else {
         ctx = new NonTxInvocationContext();
         icTl.set(ctx);
      }
      ctx.setOriginLocal(local);
      return ctx;
   }

   public InvocationContext getInvocationContext() {
      InvocationContext invocationContext = icTl.get();
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
}