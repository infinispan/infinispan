package org.infinispan.transaction.xa.recovery;

import java.util.Set;

import org.infinispan.commons.tx.Util;
import org.infinispan.commons.tx.XidImpl;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.SurvivesRestarts;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.infinispan.jmx.annotations.Parameter;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.concurrent.CompletionStages;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Admin utility class for allowing management of in-doubt transactions (e.g. transactions for which the
 * originator node crashed after prepare).
 *
 * @author Mircea Markus
 * @since 5.0
 */
@Scope(Scopes.NAMED_CACHE)
@SurvivesRestarts
@MBean(objectName = "RecoveryAdmin", description = "Exposes tooling for handling transaction recovery.")
public class RecoveryAdminOperations {

   private static final Log log = LogFactory.getLog(RecoveryAdminOperations.class);

   private static final String SEPARATOR = ", ";

   @Inject RecoveryManager recoveryManager;

   @ManagedOperation(description = "Shows all the prepared transactions for which the originating node crashed", displayName="Show in doubt transactions")
   public String showInDoubtTransactions() {
      Set<InDoubtTxInfo> info = getRecoveryInfoFromCluster();
      if (log.isTraceEnabled()) {
         log.tracef("Found in doubt transactions: %s", info.size());
      }
      StringBuilder result = new StringBuilder();
      for (InDoubtTxInfo i : info) {
         result.append("xid = [").append(i.getXid()).append("], ").append(SEPARATOR)
               .append("internalId = ").append(i.getInternalId()).append(SEPARATOR);
         result.append("status = [ ");
         int status = i.getStatus();
         if (status != -1)
            result.append(Util.transactionStatusToString(status));
         result.append(" ]");
         result.append('\n');
      }
      return result.toString();
   }

   @ManagedOperation(description = "Forces the commit of an in-doubt transaction", displayName="Force commit by internal id")
   public String forceCommit(@Parameter(name = "internalId", description = "The internal identifier of the transaction") long internalId) {
      if (log.isTraceEnabled())
         log.tracef("Forces the commit of an in-doubt transaction: %s", internalId);
      return completeBasedOnInternalId(internalId, true);
   }

   @ManagedOperation(description = "Forces the commit of an in-doubt transaction", displayName="Force commit by Xid", name="forceCommit")
   public String forceCommit(
         @Parameter(name = "formatId", description = "The formatId of the transaction") int formatId,
         @Parameter(name = "globalTxId", description = "The globalTxId of the transaction") byte[] globalTxId,
         @Parameter(name = "branchQualifier", description = "The branchQualifier of the transaction") byte[] branchQualifier) {
      return completeBasedOnXid(formatId, globalTxId, branchQualifier, true);
   }

   @ManagedOperation(description = "Forces the rollback of an in-doubt transaction", displayName="Force rollback by internal id")
   public String forceRollback(@Parameter(name = "internalId", description = "The internal identifier of the transaction") long internalId) {
      return completeBasedOnInternalId(internalId, false);
   }

   @ManagedOperation(description = "Forces the rollback of an in-doubt transaction", displayName="Force rollback by Xid", name="forceRollback")
   public String forceRollback(
         @Parameter(name = "formatId", description = "The formatId of the transaction") int formatId,
         @Parameter(name = "globalTxId", description = "The globalTxId of the transaction") byte[] globalTxId,
         @Parameter(name = "branchQualifier", description = "The branchQualifier of the transaction") byte[] branchQualifier) {
      return completeBasedOnXid(formatId, globalTxId, branchQualifier, false);
   }

   @ManagedOperation(description = "Removes recovery info for the given transaction.", displayName="Remove recovery info by Xid", name="forget")
   public String forget(
         @Parameter(name = "formatId", description = "The formatId of the transaction") int formatId,
         @Parameter(name = "globalTxId", description = "The globalTxId of the transaction") byte[] globalTxId,
         @Parameter(name = "branchQualifier", description = "The branchQualifier of the transaction") byte[] branchQualifier) {
      CompletionStages.join(recoveryManager.removeRecoveryInformation(null, XidImpl.create(formatId, globalTxId, branchQualifier), null, false));
      return "Recovery info removed.";
   }

   @ManagedOperation(description = "Removes recovery info for the given transaction.", displayName="Remove recovery info by internal id")
   public String forget(@Parameter(name = "internalId", description = "The internal identifier of the transaction") long internalId) {
      CompletionStages.join(recoveryManager.removeRecoveryInformationFromCluster(null, internalId));
      return "Recovery info removed.";
   }


   private String completeBasedOnXid(int formatId, byte[] globalTxId, byte[] branchQualifier, boolean commit) {
      InDoubtTxInfo inDoubtTxInfo = lookupRecoveryInfo(formatId, globalTxId, branchQualifier);
      if (inDoubtTxInfo != null) {
         return completeTransaction(inDoubtTxInfo, commit);
      } else {
         return transactionNotFound(formatId, globalTxId, branchQualifier);
      }
   }

   private String completeBasedOnInternalId(long internalId, boolean commit) {
      InDoubtTxInfo inDoubtTxInfo = lookupRecoveryInfo(internalId);
      if (inDoubtTxInfo != null) {
         return completeTransaction(inDoubtTxInfo, commit);
      } else {
         return transactionNotFound(internalId);
      }
   }

   private String completeTransaction(InDoubtTxInfo i, boolean commit) {
      //try to run it locally at first
      if (i.isLocal()) {
         log.tracef("Forcing completion of local transaction: %s", i);
         return CompletionStages.join(recoveryManager.forceTransactionCompletion(i.getXid(), commit));
      } else {
         log.tracef("Forcing completion of remote transaction: %s", i);
         Set<Address> owners = i.getOwners();
         if (owners == null || owners.isEmpty()) throw new IllegalStateException("Owner list cannot be empty for " + i);
         return recoveryManager.forceTransactionCompletionFromCluster(i.getXid(), owners.iterator().next(), commit);
      }
   }

   private InDoubtTxInfo lookupRecoveryInfo(int formatId, byte[] globalTxId, byte[] branchQualifier) {
      Set<InDoubtTxInfo> info = getRecoveryInfoFromCluster();
      XidImpl xid = XidImpl.create(formatId, globalTxId, branchQualifier);
      for (InDoubtTxInfo i : info) {
         if (i.getXid().equals(xid)) {
            log.tracef("Found matching recovery info: %s", i);
            return i;
         }
      }
      return null;
   }

   private Set<InDoubtTxInfo> getRecoveryInfoFromCluster() {
      Set<InDoubtTxInfo> info = recoveryManager.getInDoubtTransactionInfoFromCluster();
      log.tracef("Recovery info from cluster is: %s", info);
      return info;
   }

   private InDoubtTxInfo lookupRecoveryInfo(long internalId) {
      Set<InDoubtTxInfo> info = getRecoveryInfoFromCluster();
      for (InDoubtTxInfo i : info) {
         if (i.getInternalId() == internalId) {
            log.tracef("Found matching recovery info: %s", i);
            return i;
         }
      }
      return null;
   }

   private String transactionNotFound(int formatId, byte[] globalTxId, byte[] branchQualifier) {
      return "Transaction not found: " + XidImpl.printXid(formatId, globalTxId, branchQualifier);
   }

   private String transactionNotFound(long internalId) {
      return "Transaction not found for internal id: " + internalId;
   }
}
