/*
 * JBoss, Home of Professional Open Source
 * Copyright 2008, Red Hat Middleware LLC, and individual contributors
 * by the @authors tag. See the copyright.txt in the distribution for a
 * full listing of individual contributors.
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
package org.infinispan.commands.tx;

import org.infinispan.context.InvocationContext;
import org.infinispan.context.InvocationContextContainer;
import org.infinispan.context.impl.RemoteTxInvocationContext;
import org.infinispan.interceptors.InterceptorChain;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.transaction.xa.TransactionTable;
import org.infinispan.transaction.xa.RemoteTransaction;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * An abstract transaction boundary command that holds a reference to a {@link org.infinispan.transaction.xa.GlobalTransaction}
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
public abstract class AbstractTransactionBoundaryCommand implements TransactionBoundaryCommand {

   private static Log log = LogFactory.getLog(AbstractTransactionBoundaryCommand.class);
   private static boolean trace = log.isTraceEnabled();

   protected GlobalTransaction globalTx;
   protected String cacheName;
   protected InterceptorChain invoker;
   protected InvocationContextContainer icc;
   protected TransactionTable txTable;


   public void init(InterceptorChain chain, InvocationContextContainer icc, TransactionTable txTable) {
      this.invoker = chain;
      this.icc = icc;
      this.txTable = txTable;
   }

   public String getCacheName() {
      return cacheName;
   }

   public void setCacheName(String cacheName) {
      this.cacheName = cacheName;
   }

   public GlobalTransaction getGlobalTransaction() {
      return globalTx;
   }

   public void GlobalTransaction(GlobalTransaction gtx) {
      this.globalTx = gtx;
   }

   public Object perform(InvocationContext ctx) throws Throwable {
      if (ctx != null) throw new IllegalStateException("Expected null context!");
      RemoteTransaction transaction = txTable.getRemoteTransaction(globalTx);
      if (transaction == null) {
         if (trace) log.info("Not found RemoteTransaction for tx id: " + globalTx);
         return null;
      }
      RemoteTxInvocationContext ctxt = icc.getRemoteTxInvocationContext();
      ctxt.setRemoteTransaction(transaction);
      try {
         return invoker.invoke(ctxt, this);
      } finally {
         txTable.removeRemoteTransaction(globalTx);
      }
   }

   public Object[] getParameters() {
      return new Object[]{globalTx, cacheName};
   }

   public void setParameters(int commandId, Object[] args) {
      globalTx = (GlobalTransaction) args[0];
      cacheName = (String) args[1];
   }

   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      AbstractTransactionBoundaryCommand that = (AbstractTransactionBoundaryCommand) o;
      return this.globalTx.equals(that.globalTx);
   }

   public int hashCode() {
      return globalTx.hashCode();
   }

   @Override
   public String toString() {
      return "AbstractTransactionBoundaryCommand{" +
            "globalTx=" + globalTx +
            ", cacheName='" + cacheName + '\'' +
            ", invoker=" + invoker +
            '}';
   }
}
