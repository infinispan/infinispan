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

import org.infinispan.factories.annotations.Inject;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedOperation;
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

   public static final String SEPARAOR = ", ";

   private RecoveryManager recoveryManager;

   @Inject
   public void init(RecoveryManager recoveryManager) {
      this.recoveryManager = recoveryManager;
   }

   @ManagedOperation(description = "Shows all the prepared transactions for which the originating node crashed")
   public String showInDoubtTransactions() {
      Set<RecoveryManager.InDoubtTxInfo> info = getRecoveryInfoFromCluster();
      if (log.isTraceEnabled()) {
         log.tracef("Found in doubt transactions: %s", info.size());
      }
      StringBuilder result = new StringBuilder();
      for (RecoveryManager.InDoubtTxInfo i : info) {
         result.append("xid = [").append(i.getXid()).append("], ").append(SEPARAOR)
               .append("internalId = ").append(i.getInternalId()).append(SEPARAOR);
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

   @ManagedOperation(description = "Forces the commit of an in-doubt transaction")
   public String forceCommit(long internalID) {
      if (log.isTraceEnabled())
         log.tracef("Forces the commit of an in-doubt transaction: %s", internalID);
      return completeBasedOnInternalId(internalID, true);
   }

   @ManagedOperation(description = "Forces the commit of an in-doubt transaction")
   public String forceCommit(int formatId, byte[] globalTxId, byte[] branchQualifier) {
      return completeBasedOnXid(formatId, globalTxId, branchQualifier, true);
   }

   @ManagedOperation(description = "Forces the rollback of an in-doubt transaction")
   public String forceRollback(long internalId) {
      return completeBasedOnInternalId(internalId, false);
   }

   @ManagedOperation(description = "Forces the rollback of an in-doubt transaction")
   public String forceRollback(int formatId, byte[] globalTxId, byte[] branchQualifier) {
      return completeBasedOnXid(formatId, globalTxId, branchQualifier, false);
   }

   @ManagedOperation(description = "Removes recovery info for the given transaction.")
   public String forget(int formatId, byte[] globalTxId, byte[] branchQualifier) {
      recoveryManager.removeRecoveryInformationFromCluster(null, new SerializableXid(branchQualifier, globalTxId, formatId), true, null);
      return "Recovery info removed.";
   }

   @ManagedOperation(description = "Removes recovery info for the given transaction.")
   public String forget(long internalId) {
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
