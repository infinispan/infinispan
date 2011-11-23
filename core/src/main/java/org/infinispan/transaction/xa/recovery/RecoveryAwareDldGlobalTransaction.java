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

import static java.util.Collections.emptySet;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;

import javax.transaction.xa.Xid;

import org.infinispan.commons.marshall.Ids;
import org.infinispan.commons.util.Util;
import org.infinispan.remoting.transport.Address;
import org.infinispan.transaction.xa.DldGlobalTransaction;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.transaction.xa.TransactionFactory;

/**
 * DldGlobalTransaction that also holds xid information, required for recovery.
 * The purpose of this class is to avoid the serialization of Xid objects over the wire in the case recovery is not
 * enabled.
 *
 * @author Mircea.Markus@jboss.com
 * @since 5.0
 */
public class RecoveryAwareDldGlobalTransaction extends DldGlobalTransaction implements RecoverableTransactionIdentifier {

   public RecoveryAwareDldGlobalTransaction() {
   }

   public RecoveryAwareDldGlobalTransaction(Address addr, boolean remote) {
      super(addr, remote);
   }

   private volatile Xid xid;

   private volatile long internalId;

   @Override
   public Xid getXid() {
      return xid;
   }

   @Override
   public void setXid(Xid xid) {
      this.xid = xid;
   }

   @Override
   public long getInternalId() {
      return internalId;
   }

   @Override
   public void setInternalId(long internalId) {
      this.internalId = internalId;
   }

   public static class Externalizer extends GlobalTransaction.AbstractGlobalTxExternalizer<RecoveryAwareDldGlobalTransaction> {
      @Override
      public void writeObject(ObjectOutput output, RecoveryAwareDldGlobalTransaction globalTransaction) throws IOException {
         super.writeObject(output, globalTransaction);
         output.writeLong(globalTransaction.getCoinToss());
         if (globalTransaction.locksAtOrigin.isEmpty()) {
            output.writeObject(null);
         } else {
            output.writeObject(globalTransaction.locksAtOrigin);
         }

         output.writeObject(globalTransaction.xid);
         output.writeLong(globalTransaction.internalId);
      }

      @Override
      protected RecoveryAwareDldGlobalTransaction createGlobalTransaction() {
         return (RecoveryAwareDldGlobalTransaction) TransactionFactory.TxFactoryEnum.DLD_RECOVERY_XA.newGlobalTransaction();
      }

      @Override
      @SuppressWarnings("unchecked")
      public RecoveryAwareDldGlobalTransaction readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         RecoveryAwareDldGlobalTransaction globalTransaction = super.readObject(input);
         globalTransaction.setCoinToss(input.readLong());
         Object locksAtOriginObj = input.readObject();
         if (locksAtOriginObj == null) {
            globalTransaction.setLocksHeldAtOrigin(emptySet());
         } else {
            globalTransaction.setLocksHeldAtOrigin((Set<Object>) locksAtOriginObj);
         }

         Xid xid = (Xid) input.readObject();
         globalTransaction.setXid(xid);
         globalTransaction.setInternalId(input.readLong());
         return globalTransaction;
      }

      @Override
      public Integer getId() {
         return Ids.XID_DEADLOCK_DETECTING_GLOBAL_TRANSACTION;
      }

      @Override
      public Set<Class<? extends RecoveryAwareDldGlobalTransaction>> getTypeClasses() {
         return Util.<Class<? extends RecoveryAwareDldGlobalTransaction>>asSet(RecoveryAwareDldGlobalTransaction.class);
      }
   }

   @Override
   public String toString() {
      return getClass().getSimpleName() +
            "{xid=" + xid +
            ", internalId=" + internalId +
            "} " + super.toString();
   }
}
