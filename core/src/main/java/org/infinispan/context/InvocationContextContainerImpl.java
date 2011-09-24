/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
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
import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.VisitableCommand;
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
import java.util.Set;

/**
 * Default implementation for {@link org.infinispan.context.InvocationContextContainer}.
 *
 * @author Manik Surtani (manik AT infinispan DOT org)
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
public class InvocationContextContainerImpl implements InvocationContextContainer {

   private TransactionManager tm;
   private TransactionTable transactionTable;

   // See ISPN-1397.  There is no real need to store the InvocationContext in a thread local at all, since it is passed
   // as a parameter to any component that requires it - except for two components at the moment that require reading
   // the InvocationContext from a thread local.  These two are the ClusterCacheLoader and the JBossMarshaller.  The
   // former can be fixed once the CacheStore SPI is changed to accept an InvocationContext (see ISPN-1416) and the
   // latter can be fixed once the CacheManager architecture is changed to be associated with a ClassLoader per
   // CacheManager (see ISPN-1413), after which this thread local can be removed and the getInvocationContext() method
   // can also be removed.
   private final ThreadLocal<InvocationContext> ctxHolder = new ThreadLocal<InvocationContext>();

   @Inject
   public void init(TransactionManager tm, TransactionTable transactionTable) {
      this.tm = tm;
      this.transactionTable = transactionTable;
   }

   @Deprecated
   public InvocationContext getInvocationContext() {
      InvocationContext ctx = ctxHolder.get();
      if (ctx == null) throw new IllegalStateException("No InvocationContext associated with current thread!");
      return ctx;
   }

   public InvocationContext createInvocationContext() {
      return createInvocationContext(getRunningTx());
   }

   @Override
   public InvocationContext createInvocationContext(Transaction tx) {
      InvocationContext ctx;
      if (tx != null) {
         LocalTxInvocationContext localContext;
         localContext = new LocalTxInvocationContext();
         LocalTransaction localTransaction = transactionTable.getLocalTransaction(tx);
         localContext.setLocalTransaction(localTransaction);
         localContext.setTransaction(tx);
         ctx = localContext;
      } else {
         NonTxInvocationContext nonTxContext;
         nonTxContext = new NonTxInvocationContext();
         nonTxContext.setOriginLocal(true);
         ctx = nonTxContext;
      }
      ctxHolder.set(ctx);
      return ctx;
   }

   public LocalTxInvocationContext createTxInvocationContext() {
      LocalTxInvocationContext ctx = new LocalTxInvocationContext();
      ctxHolder.set(ctx);
      return ctx;
   }

   public RemoteTxInvocationContext createRemoteTxInvocationContext(Address origin) {
      RemoteTxInvocationContext ctx = new RemoteTxInvocationContext();
      ctx.setOrigin(origin);
      ctxHolder.set(ctx);
      return ctx;
   }

   public NonTxInvocationContext createNonTxInvocationContext() {
      NonTxInvocationContext ctx = new NonTxInvocationContext();
      ctxHolder.set(ctx);
      return ctx;
   }
   
   @Override
   public InvocationContext createRemoteInvocationContextForCommand(VisitableCommand cacheCommand, Address origin) {
      InvocationContext context = createRemoteInvocationContext(origin);
      if (cacheCommand != null && cacheCommand instanceof FlagAffectedCommand) {
         FlagAffectedCommand command = (FlagAffectedCommand) cacheCommand;
         Set<Flag> flags = command.getFlags();
         if (flags != null && !flags.isEmpty()) {
            context = new InvocationContextFlagsOverride(context, flags);
            ctxHolder.set(context);
         }
      }
      return context;
   }

   public NonTxInvocationContext createRemoteInvocationContext(Address origin) {
      NonTxInvocationContext remoteNonTxContext = new NonTxInvocationContext();
      remoteNonTxContext.setOriginLocal(false);
      remoteNonTxContext.setOrigin(origin);
      ctxHolder.set(remoteNonTxContext);
      return remoteNonTxContext;
   }

   private Transaction getRunningTx() {
      try {
         return tm == null ? null : tm.getTransaction();
      } catch (SystemException e) {
         throw new CacheException(e);
      }
   }
}