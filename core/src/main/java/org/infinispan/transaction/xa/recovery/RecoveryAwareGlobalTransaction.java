package org.infinispan.transaction.xa.recovery;

import java.io.IOException;
import java.util.Set;

import javax.transaction.xa.Xid;

import org.infinispan.commons.marshall.UserObjectInput;
import org.infinispan.commons.marshall.UserObjectOutput;
import org.infinispan.commons.util.Util;
import org.infinispan.marshall.core.Ids;
import org.infinispan.remoting.transport.Address;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.transaction.xa.TransactionFactory;

/**
 * GlobalTransaction that also holds xid information, required for recovery.
 *
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
      public void writeObject(UserObjectOutput output, RecoveryAwareGlobalTransaction xidGtx) throws IOException {
         super.writeObject(output, xidGtx);
         output.writeObject(xidGtx.xid);
         output.writeLong(xidGtx.getInternalId());
      }

      @Override
      public RecoveryAwareGlobalTransaction readObject(UserObjectInput input) throws IOException, ClassNotFoundException {
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
