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
package org.infinispan.commands.tx;

import org.infinispan.context.InvocationContext;
import org.infinispan.context.InvocationContextContainer;
import org.infinispan.context.impl.RemoteTxInvocationContext;
import org.infinispan.interceptors.InterceptorChain;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.transaction.RemoteTransaction;
import org.infinispan.transaction.TransactionTable;
import org.infinispan.remoting.transport.Address;
import org.infinispan.transaction.xa.GlobalTransaction;
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

   private static final Log log = LogFactory.getLog(AbstractTransactionBoundaryCommand.class);
   private static boolean trace = log.isTraceEnabled();

   protected GlobalTransaction globalTx;
   protected final String cacheName;
   protected InterceptorChain invoker;
   protected InvocationContextContainer icc;
   protected TransactionTable txTable;
   private Address origin;
   private int topologyId = -1;

   public AbstractTransactionBoundaryCommand(String cacheName) {
      this.cacheName = cacheName;
   }

   public void init(InterceptorChain chain, InvocationContextContainer icc, TransactionTable txTable) {
      this.invoker = chain;
      this.icc = icc;
      this.txTable = txTable;
   }

   @Override
   public int getTopologyId() {
      return topologyId;
   }

   @Override
   public void setTopologyId(int topologyId) {
      this.topologyId = topologyId;
   }

   @Override
   public String getCacheName() {
      return cacheName;
   }

   @Override
   public GlobalTransaction getGlobalTransaction() {
      return globalTx;
   }

   @Override
   public void markTransactionAsRemote(boolean isRemote) {
      globalTx.setRemote(isRemote);
   }

   /**
    * This is what is returned to remote callers when an invalid RemoteTransaction is encountered.  Can happen if a
    * remote node propagates a transactional call to the current node, and the current node has no idea of the transaction
    * in question.  Can happen during rehashing, when ownerships are reassigned during a transactions.
    *
    * Returning a null usually means the transactional command succeeded.
    * @return return value to respond to a remote caller with if the transaction context is invalid.
    */
   protected Object invalidRemoteTxReturnValue() {     //todo [anistor] no longer used
      return null;
   }

   @Override
   public Object perform(InvocationContext ctx) throws Throwable {
      if (ctx != null) throw new IllegalStateException("Expected null context!");
      markGtxAsRemote();
      RemoteTransaction transaction = txTable.getRemoteTransaction(globalTx);
      if (transaction == null) {
         if (trace) log.tracef("Did not find a RemoteTransaction for %s", globalTx);
         return invalidRemoteTxReturnValue();
      }
      visitRemoteTransaction(transaction);
      RemoteTxInvocationContext ctxt = icc.createRemoteTxInvocationContext(
            transaction, getOrigin());

      if (trace) log.tracef("About to execute tx command %s", this);
      return invoker.invoke(ctxt, this);
   }

   protected void visitRemoteTransaction(RemoteTransaction tx) {
      // to be overridden
   }

   @Override
   public Object[] getParameters() {
      return new Object[]{globalTx};
   }

   @Override
   public void setParameters(int commandId, Object[] args) {
      globalTx = (GlobalTransaction) args[0];
   }

   @Override
   public boolean shouldInvoke(InvocationContext ctx) {
      return true;
   }

   @Override
   public boolean ignoreCommandOnStatus(ComponentStatus status) {
      return false;
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
      return "gtx=" + globalTx +
            ", cacheName='" + cacheName + '\'' +
            ", topologyId=" + topologyId +
            '}';
   }

   private void markGtxAsRemote() {
      globalTx.setRemote(true);
   }
   
   @Override
   public Address getOrigin() {
	   return origin;
   }
   
   @Override
   public void setOrigin(Address origin) {
	   this.origin = origin;
   }

   @Override
   public boolean isReturnValueExpected() {
      return true;
   }
}
