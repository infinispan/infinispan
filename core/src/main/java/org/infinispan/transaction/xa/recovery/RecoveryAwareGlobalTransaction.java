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

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;

import javax.transaction.xa.Xid;

import org.infinispan.commons.marshall.Ids;
import org.infinispan.commons.util.Util;
import org.infinispan.remoting.transport.Address;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.transaction.xa.TransactionFactory;

/**
 * GlobalTransaction that also holds xid information, required for recovery.
 *
 * @see RecoveryAwareDldGlobalTransaction
 * @author Mircea.Markus@jboss.com
 * @since 5.0
 */
public class RecoveryAwareGlobalTransaction extends GlobalTransaction implements RecoverableTransactionIdentifier {

   private volatile Xid xid;

   private volatile long internalId;

   public RecoveryAwareGlobalTransaction() {
      super();
   }

   public RecoveryAwareGlobalTransaction(Address addr, boolean remote) {
      super(addr, remote);
   }

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

   @Override
   public String toString() {
      return getClass().getSimpleName() +
            "{xid=" + xid +
            ", internalId=" + internalId +
            "} " + super.toString();
   }

   public static class Externalizer extends GlobalTransaction.AbstractGlobalTxExternalizer<RecoveryAwareGlobalTransaction> {

      @Override
      protected RecoveryAwareGlobalTransaction createGlobalTransaction() {
         return (RecoveryAwareGlobalTransaction) TransactionFactory.TxFactoryEnum.NODLD_RECOVERY_XA.newGlobalTransaction();
      }

      @Override
      public void writeObject(ObjectOutput output, RecoveryAwareGlobalTransaction xidGtx) throws IOException {
         super.writeObject(output, xidGtx);
         output.writeObject(xidGtx.xid);
         output.writeLong(xidGtx.getInternalId());
      }

      @Override
      public RecoveryAwareGlobalTransaction readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         RecoveryAwareGlobalTransaction xidGtx = super.readObject(input);
         Xid xid = (Xid) input.readObject();
         xidGtx.setXid(xid);
         xidGtx.setInternalId(input.readLong());
         return xidGtx;
      }

      @Override
      public Integer getId() {
         return Ids.XID_GLOBAL_TRANSACTION;
      }

      @Override
      public Set<Class<? extends RecoveryAwareGlobalTransaction>> getTypeClasses() {
         return Util.<Class<? extends RecoveryAwareGlobalTransaction>>asSet(RecoveryAwareGlobalTransaction.class);
      }
   }

}
