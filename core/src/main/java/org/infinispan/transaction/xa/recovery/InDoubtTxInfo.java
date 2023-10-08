package org.infinispan.transaction.xa.recovery;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.commons.tx.XidImpl;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.remoting.transport.Address;

/**
 * An object describing in doubt transaction's state. Needed by the transaction recovery process, for displaying
 * transactions to the user.
 *
 * @author Mircea Markus
 * @since 5.0
 */
@ProtoTypeId(ProtoStreamTypeIds.IN_DOUBT_TX_INFO)
public class InDoubtTxInfo {

   @ProtoField(1)
   final XidImpl xid;
   @ProtoField(value = 2, defaultValue = "-1")
   final long internalId;

   @ProtoField(value = 3, defaultValue = "-1")
   int status;
   private final transient Set<Address> owners = new HashSet<>();
   private transient boolean isLocal;

   @ProtoFactory
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
}
