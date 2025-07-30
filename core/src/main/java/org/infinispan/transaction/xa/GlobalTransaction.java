package org.infinispan.transaction.xa;

import javax.transaction.xa.Xid;
import java.util.concurrent.atomic.AtomicLong;

import org.infinispan.commands.RequestUUID;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.commons.tx.XidImpl;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
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
@ProtoTypeId(ProtoStreamTypeIds.GLOBAL_TRANSACTION)
public class GlobalTransaction implements Cloneable {

   private static final AtomicLong sid = new AtomicLong(0);

   private final RequestUUID requestUUID;
   private int hash_code = -1;  // in the worst case, hashCode() returns 0, then increases, so we're safe here
   private boolean remote = false;
   private final boolean clientTx;
   private volatile XidImpl xid = null;
   private volatile long internalId = -1;

   public GlobalTransaction(Address addr, boolean remote) {
      this(addr, remote, false);
   }

   public GlobalTransaction(Address addr, boolean remote, boolean clientTx) {
      this.requestUUID = addr == null ?
            RequestUUID.localOf(sid.incrementAndGet()) :
            RequestUUID.of(addr, sid.incrementAndGet());
      this.remote = remote;
      this.clientTx = clientTx;
   }

   @ProtoFactory
   GlobalTransaction(RequestUUID requestUUID, XidImpl xid, long internalId, boolean clientTransaction) {
      this.requestUUID = requestUUID;
      this.xid = xid;
      this.internalId = internalId;
      this.clientTx = clientTransaction;
   }

   public Address getAddress() {
      return requestUUID.toAddress();
   }

   public long getId() {
      return requestUUID.getRequestId();
   }

   @ProtoField(1)
   public RequestUUID getRequestUUID() {
      return requestUUID;
   }

   @ProtoField(2)
   public XidImpl getXid() {
      return xid;
   }

   @ProtoField(3)
   public long getInternalId() {
      return internalId;
   }

   @ProtoField(4)
   public boolean isClientTransaction() {
      return clientTx;
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
         hash_code = requestUUID.hashCode();
      }
      return hash_code;
   }

   @Override
   public boolean equals(Object other) {
      if (this == other)
         return true;
      if (!(other instanceof GlobalTransaction otherGtx))
         return false;

      return requestUUID.equals(otherGtx.requestUUID);
   }

   /**
    * Returns a simplified representation of the transaction.
    */
   public final String globalId() {
      return requestUUID.toIdString();
   }

   public void setXid(Xid xid) {
      this.xid = XidImpl.copy(xid);
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
            "id=" + getId() +
            ", addr=" + getAddress() +
            ", remote=" + remote +
            ", xid=" + xid +
            ", internalId=" + internalId +
            '}';
   }
}
