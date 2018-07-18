package org.infinispan.transaction.xa;

import java.io.IOException;
import java.io.ObjectInput;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.marshall.UserObjectOutput;
import org.infinispan.commons.util.Util;
import org.infinispan.marshall.core.Ids;
import org.infinispan.remoting.transport.Address;


/**
 * Uniquely identifies a transaction that spans all JVMs in a cluster. This is used when replicating all modifications
 * in a transaction; the PREPARE and COMMIT (or ROLLBACK) messages have to have a unique identifier to associate the
 * changes with<br>. GlobalTransaction should be instantiated thorough {@link TransactionFactory} class,
 * as their type depends on the runtime configuration.
 *
 * @author <a href="mailto:bela@jboss.org">Bela Ban</a> Apr 12, 2003
 * @author <a href="mailto:manik@jboss.org">Manik Surtani (manik@jboss.org)</a>
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
public class GlobalTransaction implements Cloneable {

   private static final AtomicLong sid = new AtomicLong(0);

   protected long id = -1;

   protected Address addr = null;
   private int hash_code = -1;  // in the worst case, hashCode() returns 0, then increases, so we're safe here
   private boolean remote = false;

   /**
    * empty ctor used by externalization.
    */
   protected GlobalTransaction() {
   }

   protected GlobalTransaction(Address addr, boolean remote) {
      this.id = sid.incrementAndGet();
      this.addr = addr;
      this.remote = remote;
   }

   public Address getAddress() {
      return addr;
   }

   public long getId() {
      return id;
   }

   public boolean isRemote() {
      return remote;
   }

   public void setRemote(boolean remote) {
      this.remote = remote;
   }

   @Override
   public int hashCode() {
      if (hash_code == -1) {
         hash_code = (addr != null ? addr.hashCode() : 0) + (int) id;
      }
      return hash_code;
   }

   @Override
   public boolean equals(Object other) {
      if (this == other)
         return true;
      if (!(other instanceof GlobalTransaction))
         return false;

      GlobalTransaction otherGtx = (GlobalTransaction) other;
      boolean aeq = (addr == null) ? (otherGtx.addr == null) : addr.equals(otherGtx.addr);
      return aeq && (id == otherGtx.id);
   }

   @Override
   public String toString() {
      return "GlobalTx:" + Objects.toString(addr, "local") + ":" + id;
   }

   /**
    * Returns a simplified representation of the transaction.
    */
   public final String globalId() {
      return getAddress() + ":" + getId();
   }

   public void setId(long id) {
      this.id = id;
   }

   public void setAddress(Address address) {
      this.addr = address;
   }

   @Override
   public Object clone() {
      try {
         return super.clone();
      } catch (CloneNotSupportedException e) {
         throw new IllegalStateException("Impossible!");
      }
   }

   protected abstract static class AbstractGlobalTxExternalizer<T extends GlobalTransaction> extends AbstractExternalizer<T> {
      @Override
      public void writeObject(UserObjectOutput output, T gtx) throws IOException {
         output.writeLong(gtx.id);
         output.writeObject(gtx.addr);
      }

      /**
       * Factory method for GlobalTransactions
       * @return a newly constructed instance of GlobalTransaction or one of its subclasses
       **/
      protected abstract T createGlobalTransaction();

      @Override
      public T readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         T gtx = createGlobalTransaction();
         gtx.id = input.readLong();
         gtx.addr = (Address) input.readObject();
         return gtx;
      }
   }

   public static class Externalizer extends AbstractGlobalTxExternalizer<GlobalTransaction> {
      @Override
      @SuppressWarnings("unchecked")
      public Set<Class<? extends GlobalTransaction>> getTypeClasses() {
         return Util.<Class<? extends GlobalTransaction>>asSet(GlobalTransaction.class);
      }

      @Override
      public Integer getId() {
         return Ids.GLOBAL_TRANSACTION;
      }

      @Override
      protected GlobalTransaction createGlobalTransaction() {
         return TransactionFactory.TxFactoryEnum.NODLD_NORECOVERY_XA.newGlobalTransaction();
      }
   }
}
