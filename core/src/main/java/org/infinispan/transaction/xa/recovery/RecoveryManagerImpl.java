package org.infinispan.transaction.xa.recovery;

import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.remote.RemoveRecoveryInfoCommand;
import org.infinispan.commands.remote.GetInDoubtTransactionsCommand;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.transaction.RemoteTransaction;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.transaction.TransactionTable;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import javax.transaction.xa.Xid;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

/**
 * Default implementation for {@link RecoveryManager}
 *
 * @author Mircea.Markus@jboss.com
 * @since 5.0
 */
public class RecoveryManagerImpl implements RecoveryManager {

   private static Log log = LogFactory.getLog(RecoveryManagerImpl.class);

   private volatile RpcManager rpcManager;
   private volatile CommandsFactory commandFactory;

   /**
    * This relies on XIDs for different TMs being unique. E.g. for JBossTM this is correct as long as we have the right
    * config for <IP,port/process,sequencenumber>. This is expected to stand true for all major TM on the market.
    */
   private final ConcurrentMap<RecoveryInfoKey, RecoveryAwareRemoteTransaction> preparedTransactions;

   private final String cacheName;


   private volatile RecoveryAwareTransactionTable txTable;

   /**
    * we only broadcast the first time when node is started, then we just return the local cached prepared
    * transactions.
    */
   private volatile boolean broadcastForPreparedTx = true;

   public RecoveryManagerImpl(ConcurrentMap<RecoveryInfoKey, RecoveryAwareRemoteTransaction> recoveryHolder, String cacheName) {
      this.preparedTransactions = recoveryHolder;
      this.cacheName = cacheName;
   }

   @Inject
   public void init(RpcManager rpcManager, CommandsFactory commandsFactory, TransactionTable txTable) {
      this.rpcManager = rpcManager;
      this.commandFactory = commandsFactory;
      this.txTable = (RecoveryAwareTransactionTable)txTable;
   }

   @Override
   public RecoveryIterator getPreparedTransactionsFromCluster() {
      //1. get local transactions first
      PreparedTxIterator iterator = new PreparedTxIterator();
      iterator.add(txTable.getLocalPreparedXids());

      //2. then the remote ones
      if (notOnlyMeInTheCluster() && broadcastForPreparedTx) {
         boolean success = true;
         Map<Address, Response> responses = getAllPreparedTxFromCluster();
         for (Map.Entry<Address, Response> rEntry : responses.entrySet()) {
            Response thisResponse = rEntry.getValue();
            if (isSuccessful(thisResponse)) {
               List<Xid> responseValue = (List<Xid>) ((SuccessfulResponse) thisResponse).getResponseValue();
               if (log.isTraceEnabled()) {
                  log.trace("Received Xid lists %s from node %s", responseValue, rEntry.getKey());
               }
               iterator.add(responseValue);
            } else {
               log.warn("Missing the list of prepared transactions from node %s. Received response is %s",
                        rEntry.getKey(), rEntry.getValue());
               success = false;
            }
         }
         //this makes sure that the broadcast only happens once!
         this.broadcastForPreparedTx = !success;
         if (!broadcastForPreparedTx) log.info("Finished broadcasting for remote prepared transactions. " +
                                                     "Returning only local values from now on.");
      }
      return iterator;
   }

   @Override
   public void removeRecoveryInformation(Collection<Address> lockOwners, Xid xid, boolean sync) {
      //todo make sure this gets broad casted or at least flushed
      if (rpcManager != null) {
         RemoveRecoveryInfoCommand ftc = commandFactory.buildRemoveRecoveryInfoCommand(Collections.singletonList(xid));
         rpcManager.invokeRemotely(lockOwners, ftc, false);
      }
   }


   @Override
   public void removeLocalRecoveryInformation(List<Xid> xids) {
      for (Xid xid : xids) {
         RemoteTransaction remove = preparedTransactions.remove(new RecoveryInfoKey(xid, cacheName));
         if (remove != null && log.isTraceEnabled()) {
            log.trace("removed xid: %s", xid);
         }
      }
   }

   /**
    * A transaction is in doubt if it is {@link org.infinispan.transaction.RemoteTransaction#isInDoubt()}.
    */
   @Override
   public List<Xid> getLocalInDoubtTransactions() {
      List<Xid> result = new ArrayList<Xid>();
      for (Map.Entry<RecoveryInfoKey, RecoveryAwareRemoteTransaction> entry : preparedTransactions.entrySet()) {
         RecoveryAwareRemoteTransaction rt = entry.getValue();
         RecoveryInfoKey cacheNamePair = entry.getKey();
         if (cacheNamePair.sameCacheName(cacheName) && rt.isInDoubt()) {
            XidAware globalTransaction = (XidAware) rt.getGlobalTransaction();
            if (log.isTraceEnabled()) {
               log.trace("Found in doubt transaction: %s", globalTransaction);
            }
            result.add(globalTransaction.getXid());
         }
      }
      if (log.isTraceEnabled()) log.trace("Returning %s ", result);
      return result;
   }


   public void registerPreparedTransaction(RecoveryAwareRemoteTransaction remoteTransaction) {
      Xid xid = ((XidAware)remoteTransaction.getGlobalTransaction()).getXid();
      RemoteTransaction previous = preparedTransactions.put(new RecoveryInfoKey(xid, cacheName), remoteTransaction);
      if (previous != null) {
         log.error("There's already a prepared transaction with this xid: %s. New transaction is %s. Are there two " +
                         "different transactions having same Xid in the cluster?", previous, remoteTransaction);
         throw new IllegalStateException("Are there two different transactions having same Xid in the cluster?");
      }
   }


   public void nodesLeft(List<Address> leavers) {
      if (log.isTraceEnabled())
         log.trace("Handling leavers: %s. There are %s prepared transactions to check", leavers, preparedTransactions.values().size());
      for (RecoveryAwareRemoteTransaction rt : preparedTransactions.values()) {
         rt.computeOrphan(leavers);
      }
   }

   public void remoteTransactionCompleted(GlobalTransaction txId) {
      preparedTransactions.remove(new RecoveryInfoKey(((XidAware)txId).getXid(), cacheName));
   }

   public RemoteTransaction getPreparedTransaction(Xid xid) {
      return preparedTransactions.get(new RecoveryInfoKey(xid, cacheName));
   }

   private boolean isSuccessful(Response thisResponse) {
      return thisResponse != null && thisResponse.isValid() && thisResponse.isSuccessful();
   }

   private boolean notOnlyMeInTheCluster() {
      return rpcManager != null && rpcManager.getTransport().getMembers().size() > 1;
   }

   private Map<Address, Response> getAllPreparedTxFromCluster() {
      GetInDoubtTransactionsCommand command = commandFactory.buildGetInDoubtTransactionsCommand();
      Map<Address, Response> addressResponseMap = rpcManager.invokeRemotely(null, command, true, false);
      if (log.isTraceEnabled()) log.trace("getAllPreparedTxFromCluster received from cluster: %s", addressResponseMap);
      return addressResponseMap;
   }

   public ConcurrentMap<RecoveryInfoKey, RecoveryAwareRemoteTransaction> getPreparedTransactions() {
      return preparedTransactions;
   }
}
