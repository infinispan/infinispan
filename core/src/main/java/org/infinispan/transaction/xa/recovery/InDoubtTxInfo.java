package org.infinispan.transaction.xa.recovery;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.tx.XidImpl;
import org.infinispan.marshall.core.Ids;
import org.infinispan.remoting.transport.Address;

/**
 * An object describing in doubt transaction's state. Needed by the transaction recovery process, for displaying
 * transactions to the user.
 *
 * @author Mircea Markus
 * @since 5.0
 */
public class InDoubtTxInfo {
   public static final AbstractExternalizer<InDoubtTxInfo> EXTERNALIZER = new Externalizer();

   private final XidImpl xid;
   private final long internalId;
   private int status;
   private final transient Set<Address> owners = new HashSet<>();
   private transient boolean isLocal;

   public InDoubtTxInfo(XidImpl xid, long internalId, int status) {
      this.xid = xid;
      this.internalId = internalId;
      this.status = status;
   }

   public InDoubtTxInfo(XidImpl xid, long internalId) {
      this(xid, internalId, -1);
   }

   /**
    * @return The transaction's {@link XidImpl}.
    */
   public XidImpl getXid() {
      return xid;
   }

   /**
    * @return The unique long object associated to {@link XidImpl}. It makes possible the invocation of recovery
    * operations.
    */
   public long getInternalId() {
      return internalId;
   }

   /**
    * The value represent transaction's state as described by the {@code status} field.
    *
    * @return The {@link jakarta.transaction.Status} or -1 if not set.
    */
   public int getStatus() {
      return status;
   }

   /**
    * Sets the transaction's state.
    */
   public void setStatus(int status) {
      this.status = status;
   }

   /**
    * @return The set of nodes where this transaction information is maintained.
    */
   public Set<Address> getOwners() {
      return owners;
   }

   /**
    * Adds {@code owner} as a node where this transaction information is maintained.
    */
   public void addOwner(Address owner) {
      owners.add(owner);
   }

   /**
    * @return {@code True} if the transaction information is also present on this node.
    */
   public boolean isLocal() {
      return isLocal;
   }

   /**
    * Sets {@code true} if this transaction information is stored locally.
    */
   public void setLocal(boolean local) {
      isLocal = local;
   }


   @Override
   public boolean equals(Object o) {
      if (this == o) {
         return true;
      }
      if (o == null || getClass() != o.getClass()) {
         return false;
      }
      InDoubtTxInfo that = (InDoubtTxInfo) o;
      return internalId == that.internalId &&
            status == that.status &&
            isLocal == that.isLocal &&
            Objects.equals(xid, that.xid) &&
            Objects.equals(owners, that.owners);
   }

   @Override
   public int hashCode() {
      return Objects.hash(xid, internalId, status, owners, isLocal);
   }

   @Override
   public String toString() {
      return getClass().getSimpleName() +
            "{xid=" + xid +
            ", internalId=" + internalId +
            ", status=" + status +
            ", owners=" + owners +
            ", isLocal=" + isLocal +
            '}';
   }

   private static class Externalizer extends AbstractExternalizer<InDoubtTxInfo> {

      @Override
      public void writeObject(ObjectOutput output, InDoubtTxInfo info) throws IOException {
         XidImpl.writeTo(output, info.xid);
         output.writeLong(info.internalId);
         output.writeInt(info.status);
      }

      @Override
      public InDoubtTxInfo readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return new InDoubtTxInfo(XidImpl.readFrom(input), input.readLong(), input.readInt());
      }

      @Override
      public Integer getId() {
         return Ids.IN_DOUBT_TX_INFO;
      }

      @Override
      public Set<Class<? extends InDoubtTxInfo>> getTypeClasses() {
         return Collections.singleton(InDoubtTxInfo.class);
      }
   }
}
