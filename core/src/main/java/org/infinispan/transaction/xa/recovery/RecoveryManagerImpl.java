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
package org.infinispan.transaction.xa.recovery;

import org.infinispan.CacheException;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.remote.recovery.CompleteTransactionCommand;
import org.infinispan.commands.remote.recovery.GetInDoubtTransactionsCommand;
import org.infinispan.commands.remote.recovery.GetInDoubtTxInfoCommand;
import org.infinispan.commands.remote.recovery.TxCompletionNotificationCommand;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.transaction.LocalTransaction;
import org.infinispan.transaction.TransactionCoordinator;
import org.infinispan.transaction.TransactionTable;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.transaction.xa.LocalXaTransaction;
import org.infinispan.transaction.xa.TransactionFactory;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import javax.transaction.xa.XAException;
import javax.transaction.xa.Xid;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

/**
 * Default implementation for {@link RecoveryManager}
 *
 * @author Mircea.Markus@jboss.com
 * @since 5.0
 */
public class RecoveryManagerImpl implements RecoveryManager {

   private static final Log log = LogFactory.getLog(RecoveryManagerImpl.class);

   private volatile RpcManager rpcManager;
   private volatile CommandsFactory commandFactory;

   /**
    * This relies on XIDs for different TMs being unique. E.g. for JBossTM this is correct as long as we have the right
    * config for <IP,port/process,sequence number>. This is expected to stand true for all major TM on the market.
    */
   private final ConcurrentMap<RecoveryInfoKey, RecoveryAwareRemoteTransaction> inDoubtTransactions;

   private final String cacheName;


   private volatile RecoveryAwareTransactionTable txTable;

   private TransactionCoordinator txCoordinator;

   private TransactionFactory txFactory;
   /**
    * we only broadcast the first time when node is started, then we just return the local cached prepared
    * transactions.
    */
   private volatile boolean broadcastForPreparedTx = true;

   public RecoveryManagerImpl(ConcurrentMap<RecoveryInfoKey, RecoveryAwareRemoteTransaction> recoveryHolder, String cacheName) {
      this.inDoubtTransactions = recoveryHolder;
      this.cacheName = cacheName;
   }

   @Inject
   public void init(RpcManager rpcManager, CommandsFactory commandsFactory, TransactionTable txTable,
                    TransactionCoordinator txCoordinator, TransactionFactory txFactory) {
      this.rpcManager = rpcManager;
      this.commandFactory = commandsFactory;
      this.txTable = (RecoveryAwareTransactionTable)txTable;
      this.txCoordinator = txCoordinator;
      this.txFactory = txFactory;
   }

   @Override
   public RecoveryIterator getPreparedTransactionsFromCluster() {
      PreparedTxIterator iterator = new PreparedTxIterator();

      //1. get local transactions first
      //add the locally prepared transactions. The list of prepared transactions (even if they are not in-doubt)
      //is mandated by the recovery process according to the JTA spec: "The transaction manager calls this [i.e. recover]
      // method during recovery to obtain the list of transaction branches that are currently in prepared or heuristically
      // completed states."
      iterator.add(txTable.getLocalPreparedXids());

      //2. now also add the in-doubt transactions.
      iterator.add(getInDoubtTransactions());

      //3. then the remote ones
      if (notOnlyMeInTheCluster() && broadcastForPreparedTx) {
         boolean success = true;
         Map<Address, Response> responses = getAllPreparedTxFromCluster();
         for (Map.Entry<Address, Response> rEntry : responses.entrySet()) {
            Response thisResponse = rEntry.getValue();
            if (isSuccessful(thisResponse)) {
               List<Xid> responseValue = (List<Xid>) ((SuccessfulResponse) thisResponse).getResponseValue();
               if (log.isTraceEnabled()) {
                  log.tracef("Received Xid lists %s from node %s", responseValue, rEntry.getKey());
               }
               iterator.add(responseValue);
            } else {
               log.missingListPreparedTransactions(rEntry.getKey(), rEntry.getValue());
               success = false;
            }
         }
         //this makes sure that the broadcast only happens once!
         this.broadcastForPreparedTx = !success;
         if (!broadcastForPreparedTx)
            log.debug("Finished broadcasting for remote prepared transactions. Returning only local values from now on.");
      }
      return iterator;
   }

   @Override
   public void removeRecoveryInformationFromCluster(Collection<Address> lockOwners, Xid xid, boolean sync, GlobalTransaction gtx) {
      log.tracef("Forgetting tx information for %s", gtx);
      //todo make sure this gets broad casted or at least flushed
      if (rpcManager != null) {
         TxCompletionNotificationCommand ftc = commandFactory.buildTxCompletionNotificationCommand(xid, gtx);
         rpcManager.invokeRemotely(lockOwners, ftc, rpcManager.getDefaultRpcOptions(sync, false));
      }
      removeRecoveryInformation(xid);
   }

   @Override
   public void removeRecoveryInformationFromCluster(Collection<Address> where, long internalId, boolean sync) {
      if (rpcManager != null) {
         TxCompletionNotificationCommand ftc = commandFactory.buildTxCompletionNotificationCommand(internalId);
         rpcManager.invokeRemotely(where, ftc, rpcManager.getDefaultRpcOptions(sync, false));
      }
      removeRecoveryInformation(internalId);
   }

   @Override
   public RecoveryAwareTransaction removeRecoveryInformation(Xid xid) {
      RecoveryAwareTransaction remove = inDoubtTransactions.remove(new RecoveryInfoKey(xid, cacheName));
      log.tracef("removed in doubt xid: %s", xid);
      if (remove == null) {
         return (RecoveryAwareTransaction) txTable.removeRemoteTransaction(xid);
      }
      return remove;
   }

   @Override
   public RecoveryAwareTransaction removeRecoveryInformation(Long internalId) {
      Xid remoteTransactionXid = txTable.getRemoteTransactionXid(internalId);
      if (remoteTransactionXid != null) {
         return removeRecoveryInformation(remoteTransactionXid);
      } else {
         for (RecoveryAwareRemoteTransaction raRemoteTx : inDoubtTransactions.values()) {
            RecoverableTransactionIdentifier globalTransaction = (RecoverableTransactionIdentifier) raRemoteTx.getGlobalTransaction();
            if (internalId.equals(globalTransaction.getInternalId())) {
               Xid xid = globalTransaction.getXid();
               log.tracef("Found transaction xid %s that maps internal id %s", xid, internalId);
               removeRecoveryInformation(xid);
               return raRemoteTx;
            }
         }
      }
      log.tracef("Could not find tx to map to internal id %s", internalId);
      return null;
   }

   @Override
   public List<Xid> getInDoubtTransactions() {
      List<Xid> result = new ArrayList<Xid>();
      Set<RecoveryInfoKey> recoveryInfoKeys = inDoubtTransactions.keySet();
      for (RecoveryInfoKey key : recoveryInfoKeys) {
         if (key.cacheName.equals(cacheName))
            result.add(key.xid);
      }
      log.tracef("Returning %s ", result);
      return result;
   }

   @Override
   public Set<InDoubtTxInfo> getInDoubtTransactionInfo() {
      List<Xid> txs = getInDoubtTransactions();
      Set<RecoveryAwareLocalTransaction> localTxs = txTable.getLocalTxThatFailedToComplete();
      log.tracef("Local transactions that failed to complete is %s", localTxs);
      Set<InDoubtTxInfo> result = new HashSet<InDoubtTxInfo>();
      for (RecoveryAwareLocalTransaction r : localTxs) {
         long internalId = ((RecoverableTransactionIdentifier) r.getGlobalTransaction()).getInternalId();
         result.add(new InDoubtTxInfoImpl(r.getXid(), internalId));
      }
      for (Xid xid : txs) {
         RecoveryAwareRemoteTransaction pTx = getPreparedTransaction(xid);
         if (pTx == null) continue; //might be removed concurrently, 2check for null
         RecoverableTransactionIdentifier gtx = (RecoverableTransactionIdentifier) pTx.getGlobalTransaction();
         InDoubtTxInfoImpl infoInDoubt = new InDoubtTxInfoImpl(xid, gtx.getInternalId(), pTx.getStatus());
         result.add(infoInDoubt);
      }
      log.tracef("The set of in-doubt txs from this node is %s", result);
      return result;
   }

   @Override
   public Set<InDoubtTxInfo> getInDoubtTransactionInfoFromCluster() {
      Map<Xid, InDoubtTxInfoImpl> result = new HashMap<Xid, InDoubtTxInfoImpl>();
      if (rpcManager != null) {
         GetInDoubtTxInfoCommand inDoubtTxInfoCommand = commandFactory.buildGetInDoubtTxInfoCommand();
         Map<Address, Response> addressResponseMap = rpcManager.invokeRemotely(null, inDoubtTxInfoCommand,
                                                                               rpcManager.getDefaultRpcOptions(true));
         for (Map.Entry<Address, Response> re : addressResponseMap.entrySet()) {
            Response r = re.getValue();
            if (!isSuccessful(r)) {
               throw new CacheException("Could not fetch in doubt transactions: " + r);
            }
            Set<InDoubtTxInfoImpl> infoInDoubtSet = (Set<InDoubtTxInfoImpl>) ((SuccessfulResponse) r).getResponseValue();
            for (InDoubtTxInfoImpl infoInDoubt : infoInDoubtSet) {
               InDoubtTxInfoImpl inDoubtTxInfo = result.get(infoInDoubt.getXid());
               if (inDoubtTxInfo == null) {
                  inDoubtTxInfo = infoInDoubt;
                  result.put(infoInDoubt.getXid(), inDoubtTxInfo);
               } else {
                  inDoubtTxInfo.addStatus(infoInDoubt.getStatus());
               }
               inDoubtTxInfo.addOwner(re.getKey());
            }
         }
      }
      Set<InDoubtTxInfo> onThisNode = getInDoubtTransactionInfo();
      Iterator<InDoubtTxInfo> iterator = onThisNode.iterator();
      while (iterator.hasNext()) {
         InDoubtTxInfo info = iterator.next();
         InDoubtTxInfoImpl inDoubtTxInfo = result.get(info.getXid());
         if (inDoubtTxInfo != null) {
            inDoubtTxInfo.setLocal(true);
            iterator.remove();
         } else {
            ((InDoubtTxInfoImpl)info).setLocal(true);
         }
      }
      HashSet<InDoubtTxInfo> value = new HashSet<InDoubtTxInfo>(result.values());
      value.addAll(onThisNode);
      return value;
   }

   public void registerInDoubtTransaction(RecoveryAwareRemoteTransaction remoteTransaction) {
      Xid xid = ((RecoverableTransactionIdentifier)remoteTransaction.getGlobalTransaction()).getXid();
      RecoveryAwareTransaction previous = inDoubtTransactions.put(new RecoveryInfoKey(xid, cacheName), remoteTransaction);
      if (previous != null) {
         log.preparedTxAlreadyExists(previous, remoteTransaction);
         throw new IllegalStateException("Are there two different transactions having same Xid in the cluster?");
      }
   }


   @Override
   public RecoveryAwareRemoteTransaction getPreparedTransaction(Xid xid) {
      return inDoubtTransactions.get(new RecoveryInfoKey(xid, cacheName));
   }

   @Override
   public String forceTransactionCompletion(Xid xid, boolean commit) {
      //this means that we have this as a local transaction that originated here
      LocalXaTransaction localTransaction = txTable.getLocalTransaction(xid);
      if (localTransaction != null) {
         localTransaction.clearRemoteLocksAcquired();
         return completeTransaction(localTransaction, commit, xid);
      } else {
         RecoveryAwareRemoteTransaction tx = getPreparedTransaction(xid);
         if (tx == null) return "Could not find transaction " + xid;
         GlobalTransaction globalTransaction = tx.getGlobalTransaction();
         globalTransaction.setAddress(rpcManager.getAddress());
         globalTransaction.setRemote(false);
         RecoveryAwareLocalTransaction localTx = (RecoveryAwareLocalTransaction) txFactory.newLocalTransaction(null, globalTransaction, false, tx.getTopologyId());
         localTx.setModifications(tx.getModifications());
         localTx.setXid(xid);
         localTx.addAllAffectedKeys(tx.getAffectedKeys());
         for (Object lk : tx.getLockedKeys()) localTx.registerLockedKey(lk);
         return completeTransaction(localTx, commit, xid);
      }
   }

   private String completeTransaction(LocalTransaction localTx, boolean commit, Xid xid) {
      if (commit) {
         try {
            localTx.clearLookedUpEntries();
            txCoordinator.prepare(localTx, true);
            txCoordinator.commit(localTx, false);
         } catch (XAException e) {
            log.warn("Could not commit local tx " + localTx, e);
            return "Could not commit transaction " + xid + " : " + e.getMessage();
         }
      } else {
         try {
            txCoordinator.rollback(localTx);
         } catch (XAException e) {
            log.warn("Could not rollback local tx " + localTx, e);
            return "Could not commit transaction " + xid + " : " + e.getMessage();
         }
      }
      removeRecoveryInformationFromCluster(null, xid, false, localTx.getGlobalTransaction());
      return commit ? "Commit successful!" : "Rollback successful";
   }

   @Override
   public String forceTransactionCompletionFromCluster(Xid xid, Address where, boolean commit) {
      CompleteTransactionCommand ctc = commandFactory.buildCompleteTransactionCommand(xid, commit);
      Map<Address, Response> responseMap = rpcManager.invokeRemotely(Collections.singleton(where), ctc, rpcManager.getDefaultRpcOptions(true));
      if (responseMap.size() != 1 || responseMap.get(where) == null) {
         log.expectedJustOneResponse(responseMap);
         throw new CacheException("Expected response size is 1, received " + responseMap);
      }
      return (String) ((SuccessfulResponse) responseMap.get(where)).getResponseValue();
   }

   @Override
   public boolean isTransactionPrepared(GlobalTransaction globalTx) {
      Xid xid = ((RecoverableTransactionIdentifier) globalTx).getXid();
      RecoveryAwareRemoteTransaction remoteTransaction = (RecoveryAwareRemoteTransaction) txTable.getRemoteTransaction(globalTx);
      boolean remotePrepared = remoteTransaction != null && remoteTransaction.isPrepared();
      boolean result = inDoubtTransactions.get(new RecoveryInfoKey(xid, cacheName)) != null //if it is in doubt must be prepared
            || txTable.getLocalPreparedXids().contains(xid) || remotePrepared;
      if (log.isTraceEnabled()) log.tracef("Is tx %s prepared? %s", xid, result);
      return result;
   }

   private boolean isSuccessful(Response thisResponse) {
      return thisResponse != null && thisResponse.isValid() && thisResponse.isSuccessful();
   }

   private boolean notOnlyMeInTheCluster() {
      return rpcManager != null && rpcManager.getTransport().getMembers().size() > 1;
   }

   private Map<Address, Response> getAllPreparedTxFromCluster() {
      GetInDoubtTransactionsCommand command = commandFactory.buildGetInDoubtTransactionsCommand();
      Map<Address, Response> addressResponseMap = rpcManager.invokeRemotely(null, command, rpcManager.getDefaultRpcOptions(true));
      if (log.isTraceEnabled()) log.tracef("getAllPreparedTxFromCluster received from cluster: %s", addressResponseMap);
      return addressResponseMap;
   }

   public ConcurrentMap<RecoveryInfoKey, RecoveryAwareRemoteTransaction> getInDoubtTransactionsMap() {
      return inDoubtTransactions;
   }
}
