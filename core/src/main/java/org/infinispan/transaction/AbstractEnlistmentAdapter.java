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

package org.infinispan.transaction;

import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.remote.recovery.TxCompletionNotificationCommand;
import org.infinispan.config.Configuration;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.transaction.xa.CacheTransaction;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Collection;

/**
 * Base class for both Sync and XAResource enlistment adapters.
 *
 * @author Mircea Markus
 * @since 5.1
 */
public abstract class AbstractEnlistmentAdapter {

   private static Log log = LogFactory.getLog(AbstractEnlistmentAdapter.class);

   private final CommandsFactory commandsFactory;
   private final RpcManager rpcManager;
   private final TransactionTable txTable;
   private final ClusteringDependentLogic clusteringLogic;
   private final int hashCode;
   private final Configuration config;

   public AbstractEnlistmentAdapter(CacheTransaction cacheTransaction, CommandsFactory commandsFactory, RpcManager rpcManager, TransactionTable txTable, ClusteringDependentLogic clusteringLogic, Configuration configuration) {
      this.commandsFactory = commandsFactory;
      this.rpcManager = rpcManager;
      this.txTable = txTable;
      this.clusteringLogic = clusteringLogic;
      this.config = configuration;
      hashCode = preComputeHashCode(cacheTransaction);
   }

   public AbstractEnlistmentAdapter(CommandsFactory commandsFactory, RpcManager rpcManager, TransactionTable txTable, ClusteringDependentLogic clusteringLogic, Configuration configuration) {
      this.commandsFactory = commandsFactory;
      this.rpcManager = rpcManager;
      this.txTable = txTable;
      this.clusteringLogic = clusteringLogic;
      this.config = configuration;
      hashCode = 31;
   }

   protected final void releaseLocksForCompletedTransaction(LocalTransaction localTransaction) {
      final GlobalTransaction gtx = localTransaction.getGlobalTransaction();
      txTable.removeLocalTransaction(localTransaction);
      removeTransactionInfoRemotely(localTransaction, gtx);
   }

   private void removeTransactionInfoRemotely(LocalTransaction localTransaction, GlobalTransaction gtx) {
      if (mayHaveRemoteLocks(localTransaction) && isClustered() && !config.isSecondPhaseAsync()) {
         final TxCompletionNotificationCommand command = commandsFactory.buildTxCompletionNotificationCommand(null, gtx);
         final Collection<Address> owners = clusteringLogic.getOwners(localTransaction.getAffectedKeys());
         log.tracef("About to invoke tx completion notification on nodes %s", owners);
         rpcManager.invokeRemotely(owners, command, false);
      }
   }

   private boolean mayHaveRemoteLocks(LocalTransaction lt) {
      return (lt.getRemoteLocksAcquired() != null && !lt.getRemoteLocksAcquired().isEmpty()) ||
            (lt.getModifications() != null && !lt.getModifications().isEmpty());
   }

   /**
    * Invoked by TransactionManagers, make sure it's an efficient implementation.
    * System.identityHashCode(x) is NOT an efficient implementation.
    */
   @Override
   public final int hashCode() {
      return this.hashCode;
   }

   private static int preComputeHashCode(final CacheTransaction cacheTx) {
      return 31 + cacheTx.hashCode();
   }

   private boolean isClustered() {
      return rpcManager != null;
   }
}
