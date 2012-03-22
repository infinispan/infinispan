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
package org.infinispan.transaction.xa;

import org.infinispan.commands.CommandsFactory;
import org.infinispan.config.Configuration;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.transaction.AbstractEnlistmentAdapter;
import org.infinispan.transaction.TransactionCoordinator;
import org.infinispan.transaction.TransactionTable;
import org.infinispan.transaction.xa.recovery.RecoveryManager;
import org.infinispan.transaction.xa.recovery.SerializableXid;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

/**
 * This acts both as an local {@link org.infinispan.transaction.xa.CacheTransaction} and implementor of an {@link
 * javax.transaction.xa.XAResource} that will be called by tx manager on various tx stages.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
public class TransactionXaAdapter extends AbstractEnlistmentAdapter implements XAResource {

   private static final Log log = LogFactory.getLog(TransactionXaAdapter.class);
   private static boolean trace = log.isTraceEnabled();

   /**
    * It is really useful only if TM and client are in separate processes and TM fails. This is because a client might
    * call tm.begin and then the TM (running separate process) crashes. In this scenario the TM won't ever call
    * XAResource.rollback, so these resources would be held there forever. By knowing the timeout the RM can proceed
    * releasing the resources associated with given tx.
    */
   private int txTimeout;

   private final XaTransactionTable txTable;

   private final TransactionCoordinator txCoordinator;

   /**
    * XAResource is associated with a transaction between enlistment (XAResource.start()) XAResource.end(). It's only the
    * boundary methods (prepare, commit, rollback) that need to be "stateless".
    * Reefer to section 3.4.4 from JTA spec v.1.1
    */
   private final LocalXaTransaction localTransaction;
   private final RecoveryManager recoveryManager;
   private volatile RecoveryManager.RecoveryIterator recoveryIterator;
   private boolean recoveryEnabled;


   public TransactionXaAdapter(LocalXaTransaction localTransaction, TransactionTable txTable,
                               RecoveryManager rm, TransactionCoordinator txCoordinator,
                               CommandsFactory commandsFactory, RpcManager rpcManager,
                               ClusteringDependentLogic clusteringDependentLogic, Configuration config) {
      super(localTransaction, commandsFactory, rpcManager, txTable, clusteringDependentLogic, config);
      this.localTransaction = localTransaction;
      this.txTable = (XaTransactionTable) txTable;
      this.recoveryManager = rm;
      this.txCoordinator = txCoordinator;
      recoveryEnabled = config.isTransactionRecoveryEnabled();
   }
   public TransactionXaAdapter(TransactionTable txTable,
                               RecoveryManager rm, TransactionCoordinator txCoordinator,
                               CommandsFactory commandsFactory, RpcManager rpcManager,
                               ClusteringDependentLogic clusteringDependentLogic, Configuration config) {
      super(commandsFactory, rpcManager, txTable, clusteringDependentLogic, config);
      localTransaction = null;
      this.txTable = (XaTransactionTable) txTable;
      this.recoveryManager = rm;
      this.txCoordinator = txCoordinator;
      recoveryEnabled = config.isTransactionRecoveryEnabled();
   }

   /**
    * This can be call for any transaction object. See Section 3.4.6 (Resource Sharing) from JTA spec v1.1.
    */
   public int prepare(Xid externalXid) throws XAException {
      Xid xid = convertXid(externalXid);
      LocalXaTransaction localTransaction = getLocalTransactionAndValidate(xid);
      return txCoordinator.prepare(localTransaction);
   }

   /**
    * Same comment as for {@link #prepare(javax.transaction.xa.Xid)} applies for commit.
    */
   public void commit(Xid externalXid, boolean isOnePhase) throws XAException {
      Xid xid = convertXid(externalXid);
      LocalXaTransaction localTransaction = getLocalTransactionAndValidate(xid);
      if (isOnePhase) {
         //isOnePhase being true means that we're the only participant in the distributed transaction and TM does the
         //1PC optimization. We run a 2PC though, as running only 1PC has a high chance of leaving the cluster in
         //inconsistent state.
         txCoordinator.prepare(localTransaction);
         txCoordinator.commit(localTransaction, false);
      } else {
         txCoordinator.commit(localTransaction, false);
      }
      forgetSuccessfullyCompletedTransaction(recoveryManager, xid, localTransaction);
   }

   /**
    * Same comment as for {@link #prepare(javax.transaction.xa.Xid)} applies for commit.
    */   
   public void rollback(Xid externalXid) throws XAException {
      Xid xid = convertXid(externalXid);
      LocalXaTransaction localTransaction1 = getLocalTransactionAndValidateImpl(xid, txTable);
      localTransaction.markForRollback(true); //ISPN-879 : make sure that locks are no longer associated to this transactions
      txCoordinator.rollback(localTransaction1);
      forgetSuccessfullyCompletedTransaction(recoveryManager, xid, localTransaction1);
   }

   public void start(Xid externalXid, int i) throws XAException {
      Xid xid = convertXid(externalXid);
      //transform in our internal format in order to be able to serialize
      localTransaction.setXid(xid);
      txTable.addLocalTransactionMapping(localTransaction);
      if (trace) log.tracef("start called on tx %s", this.localTransaction.getGlobalTransaction());
   }

   public void end(Xid externalXid, int i) throws XAException {
      if (trace) log.tracef("end called on tx %s(%s)", this.localTransaction.getGlobalTransaction(), config.getName());
   }

   public void forget(Xid externalXid) throws XAException {
      Xid xid = convertXid(externalXid);
      if (trace) log.tracef("forget called for xid %s", xid);
      try {
         recoveryManager.removeRecoveryInformationFromCluster(null, xid, true, null);
      } catch (Exception e) {
         log.warn("Exception removing recovery information: ", e);
         throw new XAException(XAException.XAER_RMERR);
      }
   }

   public int getTransactionTimeout() throws XAException {
      if (trace) log.trace("start called");
      return txTimeout;
   }

   /**
    * the only situation in which it returns true is when the other xa resource pertains to the same cache, on
    * the same node.
    */
   public boolean isSameRM(XAResource xaResource) throws XAException {
      if (!(xaResource instanceof TransactionXaAdapter)) {
         return false;
      }
      TransactionXaAdapter other = (TransactionXaAdapter) xaResource;
      //there is only one tx table per cache and this is more efficient that equals.
      return this.txTable == other.txTable;
   }

   public Xid[] recover(int flag) throws XAException {
      if (!config.isTransactionRecoveryEnabled()) {
         log.recoveryIgnored();
         return RecoveryManager.RecoveryIterator.NOTHING;
      }
      if (trace) log.trace("recover called: " + flag);

      if (isFlag(flag, TMSTARTRSCAN)) {
         recoveryIterator = recoveryManager.getPreparedTransactionsFromCluster();
         if (trace) log.tracef("Fetched a new recovery iterator: %s" , recoveryIterator);
      }
      if (isFlag(flag, TMENDRSCAN)) {
         if (trace) log.trace("Flushing the iterator");
         return recoveryIterator.all();
      } else {
         //as per the spec: "TMNOFLAGS this flag must be used when no other flags are specified."
         if (!isFlag(flag, TMSTARTRSCAN) && !isFlag(flag, TMNOFLAGS))
            throw new IllegalArgumentException("TMNOFLAGS this flag must be used when no other flags are specified." +
                                                     " Received " + flag);
         return recoveryIterator.hasNext() ? recoveryIterator.next() : RecoveryManager.RecoveryIterator.NOTHING;
      }
   }

   private boolean isFlag(int value, int flag) {
      return (value & flag) != 0;
   }

   public boolean setTransactionTimeout(int i) throws XAException {
      this.txTimeout = i;
      return true;
   }

   @Override
   public String toString() {
      return "TransactionXaAdapter{" +
            "localTransaction=" + localTransaction +
            '}';
   }

   private void forgetSuccessfullyCompletedTransaction(RecoveryManager recoveryManager, Xid xid, LocalXaTransaction localTransaction) {
      final GlobalTransaction gtx = localTransaction.getGlobalTransaction();
      if (config.isTransactionRecoveryEnabled()) {
         recoveryManager.removeRecoveryInformationFromCluster(localTransaction.getRemoteLocksAcquired(), xid, false, gtx);
         txTable.removeLocalTransaction(localTransaction);
      } else {
         releaseLocksForCompletedTransaction(localTransaction);
      }
   }

   private LocalXaTransaction getLocalTransactionAndValidate(Xid xid) throws XAException {
      return getLocalTransactionAndValidateImpl(xid, txTable);
   }

   private static LocalXaTransaction getLocalTransactionAndValidateImpl(Xid xid, XaTransactionTable txTable) throws XAException {
      LocalXaTransaction localTransaction = txTable.getLocalTransaction(xid);
      if  (localTransaction == null) {
         if (trace) log.tracef("no tx found for %s", xid);
         throw new XAException(XAException.XAER_NOTA);
      }
      return localTransaction;
   }

   public LocalXaTransaction getLocalTransaction() {
      return localTransaction;
   }

   /**
    * Only does the conversion if recovery is enabled.
    */
   private Xid convertXid(Xid externalXid) {
      if (recoveryEnabled && (!(externalXid instanceof SerializableXid))) {
         return new SerializableXid(externalXid);
      } else {
         return externalXid;
      }
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      TransactionXaAdapter that = (TransactionXaAdapter) o;

      if (localTransaction != null ? !localTransaction.equals(that.localTransaction) : that.localTransaction != null)
         return false;
      //also include the name of the cache in comparison - needed when same tx spans multiple caches.
      return config.getName() != null ?
            config.getName().equals(that.config.getName()) : that.config.getName() == null;
   }
}
