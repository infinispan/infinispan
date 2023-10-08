package org.infinispan.transaction.xa.recovery;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.remote.recovery.CompleteTransactionCommand;
import org.infinispan.commands.remote.recovery.GetInDoubtTransactionsCommand;
import org.infinispan.commands.remote.recovery.GetInDoubtTxInfoCommand;
import org.infinispan.commands.remote.recovery.TxCompletionNotificationCommand;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.tx.XidImpl;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.impl.ComponentRef;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.SuccessfulCollectionResponse;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.impl.MapResponseCollector;
import org.infinispan.remoting.transport.impl.VoidResponseCollector;
import org.infinispan.transaction.impl.LocalTransaction;
import org.infinispan.transaction.impl.TransactionCoordinator;
import org.infinispan.transaction.impl.TransactionTable;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.transaction.xa.LocalXaTransaction;
import org.infinispan.transaction.xa.TransactionFactory;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Default implementation for {@link RecoveryManager}
 *
 * @author Mircea.Markus@jboss.com
 * @since 5.0
 */
@Scope(Scopes.NAMED_CACHE)
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

   private ComponentRef<TransactionTable> txTable;
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
   public void init(RpcManager rpcManager, CommandsFactory commandsFactory, ComponentRef<TransactionTable> txTable,
                    TransactionCoordinator txCoordinator, TransactionFactory txFactory) {
      this.rpcManager = rpcManager;
      this.commandFactory = commandsFactory;
      this.txTable = txTable;
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
      iterator.add((recoveryAwareTxTable()).getLocalPreparedXids());

      //2. now also add the in-doubt transactions.
      iterator.add(getInDoubtTransactions());

      //3. then the remote ones
      if (notOnlyMeInTheCluster() && broadcastForPreparedTx) {
         boolean success = true;
         Map<Address, Response> responses = getAllPreparedTxFromCluster();
         for (Map.Entry<Address, Response> rEntry : responses.entrySet()) {
            Response thisResponse = rEntry.getValue();
            if (isSuccessful(thisResponse)) {
               //noinspection unchecked
               List<XidImpl> responseValue = ((SuccessfulResponse<List<XidImpl>>) thisResponse).getResponseValue();
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
   public CompletionStage<Void> removeRecoveryInformation(Collection<Address> lockOwners, XidImpl xid, GlobalTransaction gtx,
                                         boolean fromCluster) {
      if (log.isTraceEnabled())
         log.tracef("Forgetting tx information for %s", gtx);
      //todo make sure this gets broad casted or at least flushed
      if (rpcManager != null && !fromCluster) {
         TxCompletionNotificationCommand ftc = commandFactory.buildTxCompletionNotificationCommand(xid, gtx);
         CompletionStage<Void> stage = sendTxCompletionNotification(lockOwners, ftc);
         removeRecoveryInformation(xid);
         return stage;
      } else {
         removeRecoveryInformation(xid);
         return CompletableFutures.completedNull();
      }
   }

   @Override
   public CompletionStage<Void> removeRecoveryInformationFromCluster(Collection<Address> where, long internalId) {
      if (rpcManager != null) {
         TxCompletionNotificationCommand ftc = commandFactory.buildTxCompletionNotificationCommand(internalId);
         CompletionStage<Void> stage = sendTxCompletionNotification(where, ftc);
         removeRecoveryInformation(internalId);
         return stage;
      } else {
         removeRecoveryInformation(internalId);
         return CompletableFutures.completedNull();
      }
   }

   private CompletionStage<Void> sendTxCompletionNotification(Collection<Address> where, TxCompletionNotificationCommand ftc) {
      ftc.setTopologyId(rpcManager.getTopologyId());
      if (where == null)
         return rpcManager.invokeCommandOnAll(ftc, VoidResponseCollector.ignoreLeavers(), rpcManager.getSyncRpcOptions());
      else
         return rpcManager.invokeCommand(where, ftc, VoidResponseCollector.ignoreLeavers(), rpcManager.getSyncRpcOptions());
   }

   @Override
   public RecoveryAwareTransaction removeRecoveryInformation(XidImpl xid) {
      RecoveryAwareTransaction remove = inDoubtTransactions.remove(new RecoveryInfoKey(xid, cacheName));
      log.tracef("removed in doubt xid: %s", xid);
      if (remove == null) {
         return (RecoveryAwareTransaction) recoveryAwareTxTable().removeRemoteTransaction(xid);
      }
      return remove;
   }

   @Override
   public RecoveryAwareTransaction removeRecoveryInformation(Long internalId) {
      XidImpl remoteTransactionXid = recoveryAwareTxTable().getRemoteTransactionXid(internalId);
      if (remoteTransactionXid != null) {
         return removeRecoveryInformation(remoteTransactionXid);
      } else {
         for (RecoveryAwareRemoteTransaction raRemoteTx : inDoubtTransactions.values()) {
            GlobalTransaction globalTransaction = raRemoteTx.getGlobalTransaction();
            if (internalId.equals(globalTransaction.getInternalId())) {
               XidImpl xid = globalTransaction.getXid();
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
   public List<XidImpl> getInDoubtTransactions() {
      List<XidImpl> result = inDoubtTransactions.keySet().stream()
            .filter(recoveryInfoKey -> recoveryInfoKey.cacheName.equals(cacheName))
            .map(recoveryInfoKey -> recoveryInfoKey.xid)
            .collect(Collectors.toList());
      log.tracef("Returning %s ", result);
      return result;
   }

   @Override
   public Set<InDoubtTxInfo> getInDoubtTransactionInfo() {
      Set<RecoveryAwareLocalTransaction> localTxs = recoveryAwareTxTable().getLocalTxThatFailedToComplete();
      log.tracef("Local transactions that failed to complete is %s", localTxs);
      Set<InDoubtTxInfo> result = new HashSet<>();
      for (RecoveryAwareLocalTransaction r : localTxs) {
         long internalId = r.getGlobalTransaction().getInternalId();
         result.add(new InDoubtTxInfo(r.getXid(), internalId));
      }

      for (XidImpl xid : getInDoubtTransactions()) {
         RecoveryAwareRemoteTransaction pTx = getPreparedTransaction(xid);
         if (pTx == null) continue; //might be removed concurrently, 2check for null
         GlobalTransaction gtx = pTx.getGlobalTransaction();
         InDoubtTxInfo infoInDoubt = new InDoubtTxInfo(xid, gtx.getInternalId(), pTx.getStatus());
         result.add(infoInDoubt);
      }
      log.tracef("The set of in-doubt txs from this node is %s", result);
      return result;
   }

   @Override
   public Set<InDoubtTxInfo> getInDoubtTransactionInfoFromCluster() {
      Map<XidImpl, InDoubtTxInfo> result = new HashMap<>();
      if (rpcManager != null) {
         GetInDoubtTxInfoCommand inDoubtTxInfoCommand = commandFactory.buildGetInDoubtTxInfoCommand();
         CompletionStage<Map<Address, Response>> completionStage = rpcManager.invokeCommandOnAll(inDoubtTxInfoCommand, MapResponseCollector.ignoreLeavers(),
               rpcManager.getSyncRpcOptions());
         Map<Address, Response> addressResponseMap = rpcManager.blocking(completionStage);
         for (Map.Entry<Address, Response> re : addressResponseMap.entrySet()) {
            Response r = re.getValue();
            if (!isSuccessful(r)) {
               throw new CacheException("Could not fetch in doubt transactions: " + r);
            }
            // noinspection unchecked
            Collection<InDoubtTxInfo> infoInDoubtSet = ((SuccessfulCollectionResponse<InDoubtTxInfo>) r).getResponseValue();
            for (InDoubtTxInfo infoInDoubt : infoInDoubtSet) {
               InDoubtTxInfo inDoubtTxInfo = result.get(infoInDoubt.getXid());
               if (inDoubtTxInfo == null) {
                  inDoubtTxInfo = infoInDoubt;
                  result.put(infoInDoubt.getXid(), inDoubtTxInfo);
               } else {
                  inDoubtTxInfo.setStatus(infoInDoubt.getStatus());
               }
               inDoubtTxInfo.addOwner(re.getKey());
            }
         }
      }
      Set<InDoubtTxInfo> onThisNode = getInDoubtTransactionInfo();
      Iterator<InDoubtTxInfo> iterator = onThisNode.iterator();
      while (iterator.hasNext()) {
         InDoubtTxInfo info = iterator.next();
         InDoubtTxInfo inDoubtTxInfo = result.get(info.getXid());
         if (inDoubtTxInfo != null) {
            inDoubtTxInfo.setLocal(true);
            iterator.remove();
         } else {
            info.setLocal(true);
         }
      }
      HashSet<InDoubtTxInfo> value = new HashSet<>(result.values());
      value.addAll(onThisNode);
      return value;
   }

   public void registerInDoubtTransaction(RecoveryAwareRemoteTransaction remoteTransaction) {
      XidImpl xid = remoteTransaction.getGlobalTransaction().getXid();
      RecoveryAwareTransaction previous = inDoubtTransactions.put(new RecoveryInfoKey(xid, cacheName), remoteTransaction);
      if (previous != null) {
         log.preparedTxAlreadyExists(previous, remoteTransaction);
         throw new IllegalStateException("Are there two different transactions having same Xid in the cluster?");
      }
   }


   @Override
   public RecoveryAwareRemoteTransaction getPreparedTransaction(XidImpl xid) {
      return inDoubtTransactions.get(new RecoveryInfoKey(xid, cacheName));
   }

   @Override
   public CompletionStage<String> forceTransactionCompletion(XidImpl xid, boolean commit) {
      //this means that we have this as a local transaction that originated here
      LocalXaTransaction localTransaction = recoveryAwareTxTable().getLocalTransaction(xid);
      if (localTransaction != null) {
         localTransaction.clearRemoteLocksAcquired();
         return completeTransaction(localTransaction, commit, xid);
      } else {
         RecoveryAwareRemoteTransaction tx = getPreparedTransaction(xid);
         if (tx == null) return CompletableFuture.completedFuture("Could not find transaction " + xid);
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

   private CompletionStage<String> completeTransaction(LocalTransaction localTx, boolean commit, XidImpl xid) {
      GlobalTransaction gtx = localTx.getGlobalTransaction();
      if (commit) {
         localTx.clearLookedUpEntries();
         return txCoordinator.prepare(localTx, true)
               .thenCompose(ignore -> txCoordinator.commit(localTx, false))
               .thenCompose(ignore -> removeRecoveryInformation(null, xid, gtx, false))
               .thenApply(ignore -> "Commit successful!")
               .exceptionally(t -> {
                  log.warnCouldNotCommitLocalTx(localTx, t);
                  return "Could not commit transaction " + xid + " : " + t.getMessage();
               });
      } else {
         return txCoordinator.rollback(localTx)
               .thenCompose(ignore -> removeRecoveryInformation(null, xid, gtx, false))
               .thenApply(ignore -> "Rollback successful")
               .exceptionally(t -> {
                  log.warnCouldNotRollbackLocalTx(localTx, t);
                  return "Could not rollback transaction " + xid + " : " + t.getMessage();
               });
      }
   }

   @Override
   public String forceTransactionCompletionFromCluster(XidImpl xid, Address where, boolean commit) {
      CompleteTransactionCommand ctc = commandFactory.buildCompleteTransactionCommand(xid, commit);
      CompletionStage<Map<Address, Response>> completionStage = rpcManager.invokeCommand(where, ctc, MapResponseCollector.validOnly(), rpcManager.getSyncRpcOptions());
      Map<Address, Response> responseMap = rpcManager.blocking(completionStage);
      if (responseMap.size() != 1 || responseMap.get(where) == null) {
         log.expectedJustOneResponse(responseMap);
         throw new CacheException("Expected response size is 1, received " + responseMap);
      }
      //noinspection rawtypes
      return (String) ((SuccessfulResponse) responseMap.get(where)).getResponseValue();
   }

   @Override
   public boolean isTransactionPrepared(GlobalTransaction globalTx) {
      XidImpl xid = globalTx.getXid();
      RecoveryAwareRemoteTransaction remoteTransaction = (RecoveryAwareRemoteTransaction) recoveryAwareTxTable().getRemoteTransaction(globalTx);
      boolean remotePrepared = remoteTransaction != null && remoteTransaction.isPrepared();
      boolean result = inDoubtTransactions.get(new RecoveryInfoKey(xid, cacheName)) != null//if it is in doubt must be prepared
                       || recoveryAwareTxTable().getLocalPreparedXids().contains(xid) || remotePrepared;
      if (log.isTraceEnabled()) log.tracef("Is tx %s prepared? %s", xid, result);
      return result;
   }

   private RecoveryAwareTransactionTable recoveryAwareTxTable() {
      return (RecoveryAwareTransactionTable) txTable.running();
   }

   private boolean isSuccessful(Response thisResponse) {
      return thisResponse != null && thisResponse.isValid() && thisResponse.isSuccessful();
   }

   private boolean notOnlyMeInTheCluster() {
      return rpcManager != null && rpcManager.getTransport().getMembers().size() > 1;
   }

   private Map<Address, Response> getAllPreparedTxFromCluster() {
      GetInDoubtTransactionsCommand command = commandFactory.buildGetInDoubtTransactionsCommand();
      CompletionStage<Map<Address, Response>> completionStage = rpcManager.invokeCommandOnAll(command, MapResponseCollector.ignoreLeavers(), rpcManager.getSyncRpcOptions());
      Map<Address, Response> addressResponseMap = rpcManager.blocking(completionStage);
      if (log.isTraceEnabled()) log.tracef("getAllPreparedTxFromCluster received from cluster: %s", addressResponseMap);
      return addressResponseMap;
   }

   public ConcurrentMap<RecoveryInfoKey, RecoveryAwareRemoteTransaction> getInDoubtTransactionsMap() {
      return inDoubtTransactions;
   }
}
