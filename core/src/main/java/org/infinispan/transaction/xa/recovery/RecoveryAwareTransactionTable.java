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
import org.infinispan.remoting.transport.Address;
import org.infinispan.transaction.LocalTransaction;
import org.infinispan.transaction.RemoteTransaction;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.transaction.xa.LocalXaTransaction;
import org.infinispan.transaction.xa.XaTransactionTable;

import javax.transaction.xa.Xid;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Transaction table that delegates prepared transaction's management to the {@link RecoveryManager}.
 *
 * @author Mircea.Markus@jboss.com
 * @since 5.0
 */
public class RecoveryAwareTransactionTable extends XaTransactionTable {

   private RecoveryManagerImpl recoveryManager;

   @Inject
   public void initialize(RecoveryManager recoveryManager) {
      this.recoveryManager = (RecoveryManagerImpl) recoveryManager;
   }

   @Override
   public void remoteTransactionPrepared(GlobalTransaction gtx) {
      RecoveryAwareRemoteTransaction remoteTransaction = (RecoveryAwareRemoteTransaction) remoteTransactions.get(gtx);
      remoteTransaction.setPrepared(true);
      RemoteTransaction preparedTx = remoteTransactions.remove(remoteTransaction.getGlobalTransaction());
      if (preparedTx == null)
         throw new IllegalStateException("This tx has just been prepared, cannot be missing from here!");
      recoveryManager.registerPreparedTransaction(remoteTransaction);
   }

   @Override
   public void localTransactionPrepared(LocalTransaction localTransaction) {
      ((RecoveryAwareLocalTransaction) localTransaction).setPrepared(true);
   }

   @Override
   protected void updateStateOnNodesLeaving(List<Address> leavers) {
      recoveryManager.nodesLeft(leavers);
      super.updateStateOnNodesLeaving(leavers);
   }

   @Override
   public RemoteTransaction getRemoteTransaction(GlobalTransaction txId) {
      RemoteTransaction remoteTransaction = remoteTransactions.get(txId);
      if (remoteTransaction != null) return remoteTransaction;
      //also look in the recovery manager, as this transaction might be prepared
      return recoveryManager.getPreparedTransaction(((XidAware) txId).getXid());
   }

   @Override
   public void remoteTransactionCompleted(GlobalTransaction gtx) {
      //ignore the call, the transaction will be removed async at a further point in time
   }

   public List<Xid> getLocalPreparedXids() {
      List<Xid> result = new LinkedList<Xid>();
      for (Map.Entry<Xid, LocalXaTransaction> e : xid2LocalTx.entrySet()) {
         RecoveryAwareLocalTransaction value = (RecoveryAwareLocalTransaction) e.getValue();
         if (value.isPrepared()) {
            result.add(e.getKey());
         }
      }
      return result;
   }
}
