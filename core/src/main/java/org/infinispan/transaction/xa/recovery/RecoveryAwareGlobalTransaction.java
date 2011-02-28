package org.infinispan.transaction.xa.recovery;

import org.infinispan.marshall.AbstractExternalizer;
import org.infinispan.marshall.Ids;
import org.infinispan.remoting.transport.Address;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.transaction.xa.TransactionFactory;
import org.infinispan.util.Util;

import javax.transaction.xa.Xid;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;

/**
 * GlobalTransaction that also holds xid information, required for recovery.
 *
 * @see RecoveryAwareDldGlobalTransaction
 * @author Mircea.Markus@jboss.com
 * @since 5.0
 */
public class RecoveryAwareGlobalTransaction extends GlobalTransaction implements XidAware {

   private volatile Xid xid;

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

   public static class Externalizer extends AbstractExternalizer<RecoveryAwareGlobalTransaction> {
      private final GlobalTransaction.Externalizer delegate = new GlobalTransaction.Externalizer(new TransactionFactory(false, true));

      @Override
      public void writeObject(ObjectOutput output, RecoveryAwareGlobalTransaction xidGtx) throws IOException {
         delegate.writeObject(output, xidGtx);
         output.writeObject(xidGtx.xid);
      }

      @Override
      @SuppressWarnings("unchecked")
      public RecoveryAwareGlobalTransaction readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         RecoveryAwareGlobalTransaction xidGtx = (RecoveryAwareGlobalTransaction) delegate.readObject(input);
         Xid xid = (Xid) input.readObject();
         xidGtx.setXid(xid);
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
