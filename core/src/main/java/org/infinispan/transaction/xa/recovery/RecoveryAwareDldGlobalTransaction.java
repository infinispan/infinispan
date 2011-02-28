package org.infinispan.transaction.xa.recovery;

import org.infinispan.marshall.AbstractExternalizer;
import org.infinispan.marshall.Ids;
import org.infinispan.remoting.transport.Address;
import org.infinispan.transaction.xa.DldGlobalTransaction;
import org.infinispan.transaction.xa.TransactionFactory;
import org.infinispan.util.Util;

import javax.transaction.xa.Xid;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;

/**
 * DldGlobalTransaction that also holds xid information, required for recovery.
 * The purpose of this class is to avoid the serialization of Xid objects over the wire in the case recovery is not
 * enabled.
 *
 * @author Mircea.Markus@jboss.com
 * @since 5.0
 */
public class RecoveryAwareDldGlobalTransaction extends DldGlobalTransaction implements XidAware {

   public RecoveryAwareDldGlobalTransaction() {
   }

   public RecoveryAwareDldGlobalTransaction(Address addr, boolean remote) {
      super(addr, remote);
   }

   private volatile Xid xid;

   @Override
   public Xid getXid() {
      return xid;
   }

   @Override
   public void setXid(Xid xid) {
      this.xid = xid;
   }

   public static class Externalizer extends AbstractExternalizer<RecoveryAwareDldGlobalTransaction> {
      private final DldGlobalTransaction.Externalizer delegate = new DldGlobalTransaction.Externalizer(new TransactionFactory(true, true));

      @Override
      public void writeObject(ObjectOutput output, RecoveryAwareDldGlobalTransaction xidGtx) throws IOException {
         delegate.writeObject(output, xidGtx);
         output.writeObject(xidGtx.xid);
      }

      @Override
      @SuppressWarnings("unchecked")
      public RecoveryAwareDldGlobalTransaction readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         RecoveryAwareDldGlobalTransaction xidGtx = (RecoveryAwareDldGlobalTransaction) delegate.readObject(input);
         Xid xid = (Xid) input.readObject();
         xidGtx.setXid(xid);
         return xidGtx;
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
}
