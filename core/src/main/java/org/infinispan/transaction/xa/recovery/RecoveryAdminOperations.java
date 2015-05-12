package org.infinispan.transaction.xa.recovery;

import org.infinispan.factories.annotations.Inject;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.infinispan.jmx.annotations.Parameter;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import javax.transaction.Status;
import javax.transaction.xa.Xid;
import java.util.Set;

/**
 * Admin utility class for allowing management of in-doubt transactions (e.g. transactions for which the
 * originator node crashed after prepare).
 *
 * @author Mircea Markus
 * @since 5.0
 */
@MBean(objectName = "RecoveryAdmin", description = "Exposes tooling for handling transaction recovery.")
public class RecoveryAdminOperations {

   private static final Log log = LogFactory.getLog(RecoveryAdminOperations.class);

   public static final String SEPARATOR = ", ";

   private RecoveryManager recoveryManager;

   @Inject
   public void init(RecoveryManager recoveryManager) {
      this.recoveryManager = recoveryManager;
   }

   @ManagedOperation(description = "Shows all the prepared transactions for which the originating node crashed", displayName="Show in doubt transactions")
   public String showInDoubtTransactions() {
      Set<RecoveryManager.InDoubtTxInfo> info = getRecoveryInfoFromCluster();
      if (log.isTraceEnabled()) {
         log.tracef("Found in doubt transactions: %s", info.size());
      }
      StringBuilder result = new StringBuilder();
      for (RecoveryManager.InDoubtTxInfo i : info) {
         result.append("xid = [").append(i.getXid()).append("], ").append(SEPARATOR)
               .append("internalId = ").append(i.getInternalId()).append(SEPARATOR);
         result.append("status = [ ");
         for (Integer status : i.getStatus()) {
            if (status == Status.STATUS_PREPARED) {
               result.append("_PREPARED_");
            } else if (status == Status.STATUS_COMMITTED) {
               result.append("_COMMITTED_");
            } else if (status == Status.STATUS_ROLLEDBACK) {
               result.append("_ROLLEDBACK_");
            }
         }
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
      recoveryManager.removeRecoveryInformation(null, new SerializableXid(branchQualifier, globalTxId, formatId), true, null, false);
      return "Recovery info removed.";
   }

   @ManagedOperation(description = "Removes recovery info for the given transaction.", displayName="Remove recovery info by internal id")
   public String forget(@Parameter(name = "internalId", description = "The internal identifier of the transaction") long internalId) {
      recoveryManager.removeRecoveryInformationFromCluster(null, internalId, true);
      return "Recovery info removed.";
   }


   private String completeBasedOnXid(int formatId, byte[] globalTxId, byte[] branchQualifier, boolean commit) {
      RecoveryManager.InDoubtTxInfo inDoubtTxInfo = lookupRecoveryInfo(formatId, globalTxId, branchQualifier);
      if (inDoubtTxInfo != null) {
         return completeTransaction(inDoubtTxInfo.getXid(), inDoubtTxInfo, commit);
      } else {
         return transactionNotFound(formatId, globalTxId, branchQualifier);
      }
   }

   private String completeBasedOnInternalId(Long internalId, boolean commit) {
      RecoveryManager.InDoubtTxInfo inDoubtTxInfo = lookupRecoveryInfo(internalId);
      if (inDoubtTxInfo != null) {
         return completeTransaction(inDoubtTxInfo.getXid(), inDoubtTxInfo, commit);
      } else {
         return transactionNotFound(internalId);
      }
   }

   private String completeTransaction(Xid xid, RecoveryManager.InDoubtTxInfo i, boolean commit) {
      //try to run it locally at first
      if (i.isLocal()) {
         log.tracef("Forcing completion of local transaction: %s", i);
         return recoveryManager.forceTransactionCompletion(xid, commit);
      } else {
         log.tracef("Forcing completion of remote transaction: %s", i);
         Set<Address> owners = i.getOwners();
         if (owners == null || owners.isEmpty()) throw new IllegalStateException("Owner list cannot be empty for " + i);
         return recoveryManager.forceTransactionCompletionFromCluster(xid, owners.iterator().next(), commit);
      }
   }

   private  RecoveryManager.InDoubtTxInfo lookupRecoveryInfo(int formatId, byte[] globalTxId, byte[] branchQualifier) {
      Set<RecoveryManager.InDoubtTxInfo> info = getRecoveryInfoFromCluster();
      SerializableXid xid = new SerializableXid(branchQualifier, globalTxId, formatId);
      for (RecoveryManager.InDoubtTxInfo i : info) {
         if (i.getXid().equals(xid)) {
            log.tracef("Found matching recovery info: %s", i);
            return i;
         }
      }
      return null;
   }

   private Set<RecoveryManager.InDoubtTxInfo> getRecoveryInfoFromCluster() {
      Set<RecoveryManager.InDoubtTxInfo> info = recoveryManager.getInDoubtTransactionInfoFromCluster();
      log.tracef("Recovery info from cluster is: %s", info);
      return info;
   }

   private RecoveryManager.InDoubtTxInfo lookupRecoveryInfo(Long internalId) {
      Set<RecoveryManager.InDoubtTxInfo> info = getRecoveryInfoFromCluster();
      for (RecoveryManager.InDoubtTxInfo i : info) {
         if (i.getInternalId().equals(internalId)) {
            log.tracef("Found matching recovery info: %s", i);
            return i;
         }
      }
      return null;
   }

   private String transactionNotFound(int formatId, byte[] globalTxId, byte[] branchQualifier) {
      return "Transaction not found: " + new SerializableXid(branchQualifier, globalTxId, formatId);
   }

   private String transactionNotFound(Long internalId) {
      return "Transaction not found for internal id: " + internalId;
   }
}
