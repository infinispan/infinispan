package org.infinispan.transaction.xa.recovery;

import java.io.IOException;
import java.io.ObjectInput;
import java.util.Collections;
import java.util.Set;

import javax.transaction.xa.Xid;

import org.infinispan.commons.marshall.UserObjectOutput;
import org.infinispan.commons.util.Util;
import org.infinispan.marshall.core.Ids;
import org.infinispan.remoting.transport.Address;
import org.infinispan.transaction.xa.DldGlobalTransaction;
import org.infinispan.transaction.xa.GlobalTransaction;

/**
 * DldGlobalTransaction that also holds xid information, required for recovery.
 * The purpose of this class is to avoid the serialization of Xid objects over the wire in the case recovery is not
 * enabled.
 *
 * @author Mircea.Markus@jboss.com
 * @since 5.0
 * @deprecated Since 9.0, no longer used.
 */
@Deprecated
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

   @Deprecated
   public static class Externalizer extends GlobalTransaction.AbstractGlobalTxExternalizer<RecoveryAwareDldGlobalTransaction> {
      @Override
      public void writeObject(UserObjectOutput output, RecoveryAwareDldGlobalTransaction globalTransaction) throws IOException {
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
         return new RecoveryAwareDldGlobalTransaction();
      }

      @Override
      @SuppressWarnings("unchecked")
      public RecoveryAwareDldGlobalTransaction readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         RecoveryAwareDldGlobalTransaction globalTransaction = super.readObject(input);
         globalTransaction.setCoinToss(input.readLong());
         Object locksAtOriginObj = input.readObject();
         if (locksAtOriginObj == null) {
            globalTransaction.setLocksHeldAtOrigin(Collections.emptySet());
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
