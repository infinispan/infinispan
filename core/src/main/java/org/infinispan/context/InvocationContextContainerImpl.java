/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2000 - 2008, Red Hat Middleware LLC, and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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
import org.infinispan.transaction.xa.TransactionXaAdapter;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.transaction.xa.TransactionTable;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


/**
 * Container and factory for thread locals
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @since 4.0
 */
public class InvocationContextContainerImpl implements InvocationContextContainer {

   private static Log log = LogFactory.getLog(InvocationContextContainerImpl.class);

   private TransactionManager tm;
   private TransactionTable transactionTable;


   private ThreadLocal<PossibleContexts> contextsTl = new ThreadLocal<PossibleContexts>() {
      @Override
      protected PossibleContexts initialValue() {
         return new PossibleContexts();
      }
   };

   private Map<GlobalTransaction, RemoteTxInvocationContext> remoteTxMap = new ConcurrentHashMap<GlobalTransaction, RemoteTxInvocationContext>(20);

   @Inject
   public void init(TransactionManager tm, TransactionTable transactionTable) {
      this.tm = tm;
      this.transactionTable = transactionTable;
   }

   public InvocationContext getLocalInvocationContext() {
      PossibleContexts contexts = contextsTl.get();
      Transaction tx = getRunningTx();
      if (tx != null) {
         contexts.initInitiatorInvicationContext();
         LocalTxInvocationContext context = contexts.localTxInvocationContext;
         TransactionXaAdapter xaAdapter = transactionTable.getXaCacheAdapter(tx);
         context.setXaCache(xaAdapter);
         return contexts.updateThreadContextAndReturn(context);
      } else {
         contexts.initNonTxInvocationContext();
         contexts.nonTxInvocationContext.prepareForCall();
         contexts.nonTxInvocationContext.setOriginLocal(true);
         return contexts.updateThreadContextAndReturn(contexts.nonTxInvocationContext);
      }
   }

   public LocalTxInvocationContext getInitiatorTxInvocationContext() {
      PossibleContexts contexts = contextsTl.get();
      contexts.initInitiatorInvicationContext();
      contexts.updateThreadContextAndReturn(contexts.localTxInvocationContext);
      return contexts.localTxInvocationContext;
   }

   public RemoteTxInvocationContext getRemoteTxInvocationContext() {
      PossibleContexts contexts = contextsTl.get();
      contexts.initRemoteTxInvocationContext();
      contexts.updateThreadContextAndReturn(contexts.remoteTxContext);
      return contexts.remoteTxContext;
   }

   public InvocationContext getRemoteNonTxInvocationContext() {
      PossibleContexts contexts = contextsTl.get();
      contexts.initRemoteNonTxInvocationContext();
      contexts.remoteNonTxContext.prepareForCall();
      return contexts.updateThreadContextAndReturn(contexts.remoteNonTxContext);
   }

   public InvocationContext getThreadContext() {
      InvocationContext invocationContext = contextsTl.get().threadInvocationContex;
      if (invocationContext == null)
         throw new IllegalStateException("This method can only be called after associating the current thread with a context");
      return invocationContext;
   }


   public Object suspend() {
      PossibleContexts result = contextsTl.get();
      contextsTl.remove();
      return result;
   }

   public void resume(Object backup) {
      contextsTl.set((PossibleContexts) backup);
   }

   public static class PossibleContexts {
      private NonTxInvocationContext nonTxInvocationContext;
      private LocalTxInvocationContext localTxInvocationContext;
      private NonTxInvocationContext remoteNonTxContext;
      private RemoteTxInvocationContext remoteTxContext;
      private InvocationContext threadInvocationContex;

      public void initInitiatorInvicationContext() {
         if (localTxInvocationContext == null) {
            localTxInvocationContext = new LocalTxInvocationContext();
         }
      }

      public void initNonTxInvocationContext() {
         if (nonTxInvocationContext == null) {
            nonTxInvocationContext = new NonTxInvocationContext();
         }
      }

      public void initRemoteNonTxInvocationContext() {
         if (remoteNonTxContext == null) {
            remoteNonTxContext = new NonTxInvocationContext();
         }
      }

      public void initRemoteTxInvocationContext() {
         if (remoteTxContext == null) {
            remoteTxContext = new RemoteTxInvocationContext();
         }
      }

      public InvocationContext updateThreadContextAndReturn(InvocationContext ic) {
         threadInvocationContex = ic;
         return ic;
      }
   }

   private Transaction getRunningTx() {
      try {
         return tm == null ? null : tm.getTransaction();
      } catch (SystemException e) {
         throw new CacheException(e);
      }
   }
}