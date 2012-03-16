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
package org.infinispan.commands.remote.recovery;

import org.infinispan.context.InvocationContext;
import org.infinispan.transaction.RemoteTransaction;
import org.infinispan.transaction.TransactionTable;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.transaction.xa.recovery.RecoveryManager;
import org.infinispan.util.concurrent.locks.LockManager;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import javax.transaction.xa.Xid;

/**
 * Command for removing recovery related information from the cluster.
 *
 * @author Mircea.Markus@jboss.com
 * @since 5.0
 */
public class TxCompletionNotificationCommand extends RecoveryCommand {

   private static Log log = LogFactory.getLog(TxCompletionNotificationCommand.class);

   public static final int COMMAND_ID = 22;

   private Xid xid;
   private long internalId;
   private GlobalTransaction gtx;
   private TransactionTable txTable;
   private LockManager lockManager;

   private TxCompletionNotificationCommand() {
      super(null); // For command id uniqueness test
   }

   public TxCompletionNotificationCommand(Xid xid, GlobalTransaction gtx, String cacheName) {
      super(cacheName);
      this.xid = xid;
      this.gtx = gtx;
   }

   public void init(TransactionTable tt, LockManager lockManager, RecoveryManager rm) {
      super.init(rm);
      this.txTable = tt;
      this.lockManager = lockManager;
   }


   public TxCompletionNotificationCommand(long internalId, String cacheName) {
      super(cacheName);
      this.internalId = internalId;
   }

   public TxCompletionNotificationCommand(String cacheName) {
      super(cacheName);
   }

   @Override
   public Object perform(InvocationContext ctx) throws Throwable {
      log.tracef("Processing completed transaction %s", gtx);
      RemoteTransaction remoteTx = null;
      if (recoveryManager != null) { //recovery in use
         if (xid != null) {
            remoteTx = (RemoteTransaction) recoveryManager.removeRecoveryInformation(xid);
         } else {
            remoteTx = (RemoteTransaction) recoveryManager.removeRecoveryInformation(internalId);
         }
      }
      if (remoteTx == null && gtx != null) {
         remoteTx = txTable.removeRemoteTransaction(gtx);
      }
      if (remoteTx == null) return null;
      lockManager.unlock(remoteTx.getLockedKeys(), remoteTx.getGlobalTransaction());
      return null;
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public Object[] getParameters() {
      return new Object[]{xid != null ? xid : internalId, gtx};
   }

   @Override
   public void setParameters(int commandId, Object[] parameters) {
      if (commandId != COMMAND_ID) {
         throw new IllegalArgumentException("Wrong command id. Received " + commandId + " and expected " + TxCompletionNotificationCommand.COMMAND_ID);
      }
      if (parameters[0] instanceof Xid) {
         xid = (Xid) parameters[0];
      } else {
         internalId = (Long) parameters[0];
      }
      gtx = (GlobalTransaction) parameters[1];
   }

   @Override
   public String toString() {
      return getClass().getSimpleName() +
            "{ xid=" + xid +
            ", internalId=" + internalId +
            ", gtx=" + gtx +
            ", cacheName=" + cacheName + "} ";
   }
}
