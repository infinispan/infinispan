package org.infinispan.transaction.xa;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.tx.XidImpl;
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

   private long id;
   private Address addr;
   private int hash_code = -1;  // in the worst case, hashCode() returns 0, then increases, so we're safe here
   private boolean remote = false;
   private volatile XidImpl xid = null;
   private volatile long internalId = -1;

   public GlobalTransaction(Address addr, boolean remote) {
      this.id = sid.incrementAndGet();
      this.addr = addr;
      this.remote = remote;
   }

   private GlobalTransaction(long id, Address addr, XidImpl xid, long internalId) {
      this.id = id;
      this.addr = addr;
      this.xid = xid;
      this.internalId = internalId;
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
      return id == otherGtx.id &&
            Objects.equals(addr, otherGtx.addr);
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

   public XidImpl getXid() {
      return xid;
   }

   public void setXid(XidImpl xid) {
      this.xid = xid;
   }

   public long getInternalId() {
      return internalId;
   }

   public void setInternalId(long internalId) {
      this.internalId = internalId;
   }

   @Override
   public Object clone() {
      try {
         return super.clone();
      } catch (CloneNotSupportedException e) {
         throw new IllegalStateException("Impossible!");
      }
   }

   @Override
   public String toString() {
      return "GlobalTransaction{" +
            "id=" + id +
            ", addr=" + Objects.toString(addr, "local") +
            ", remote=" + remote +
            ", xid=" + xid +
            ", internalId=" + internalId +
            '}';
   }

   public static class Externalizer implements AdvancedExternalizer<GlobalTransaction> {
      @Override
      public Set<Class<? extends GlobalTransaction>> getTypeClasses() {
         return Util.asSet(GlobalTransaction.class);
      }

      @Override
      public Integer getId() {
         return Ids.GLOBAL_TRANSACTION;
      }

      @Override
      public void writeObject(ObjectOutput output, GlobalTransaction gtx) throws IOException {
         output.writeLong(gtx.id);
         output.writeObject(gtx.addr);
         output.writeObject(gtx.xid);
         output.writeLong(gtx.internalId);
      }

      @Override
      public GlobalTransaction readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         long id = input.readLong();
         Address addr = (Address) input.readObject();
         XidImpl xid = (XidImpl) input.readObject();
         long internalId = input.readLong();
         return new GlobalTransaction(id, addr, xid, internalId);
      }
   }
}
