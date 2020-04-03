package org.infinispan.transaction.xa.recovery;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import javax.transaction.xa.Xid;

import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.util.Util;
import org.infinispan.marshall.core.Ids;
import org.infinispan.remoting.transport.Address;

/**
* @author Mircea Markus
* @since 5.0
*/
public class InDoubtTxInfoImpl implements RecoveryManager.InDoubtTxInfo {
   private Xid xid;
   private long internalId;
   private int status;
   private transient Set<Address> owners = new HashSet<>();
   private transient boolean isLocal;

   public InDoubtTxInfoImpl(Xid xid, long internalId, int status) {
      this.xid = xid;
      this.internalId = internalId;
      this.status = status;
   }

   public InDoubtTxInfoImpl(Xid xid, long internalId) {
      this(xid, internalId, -1);
   }

   @Override
   public Xid getXid() {
      return xid;
   }

   @Override
   public long getInternalId() {
      return internalId;
   }

   @Override
   public int getStatus() {
      return status;
   }

   @Override
   public Set<Address> getOwners() {
      return owners;
   }

   public void addStatus(int status) {
      this.status = status;
   }

   public void addOwner(Address owner) {
      owners.add(owner);
   }

   @Override
   public boolean isLocal() {
      return isLocal;
   }

   public void setLocal(boolean local) {
      isLocal = local;
   }

   public static class Externalizer extends AbstractExternalizer<InDoubtTxInfoImpl> {

      public Externalizer() {
      }

      @Override
      public void writeObject(ObjectOutput output, InDoubtTxInfoImpl inDoubtTxInfoImpl) throws IOException {
         output.writeObject(inDoubtTxInfoImpl.getXid());
         output.writeLong(inDoubtTxInfoImpl.getInternalId());
         output.writeInt(inDoubtTxInfoImpl.status);
      }

      @Override
      public InDoubtTxInfoImpl readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return new InDoubtTxInfoImpl((Xid) input.readObject(), input.readLong(), input.readInt());
      }

      @Override
      public Integer getId() {
         return Ids.IN_DOUBT_TX_INFO;
      }

      @Override
      public Set<Class<? extends InDoubtTxInfoImpl>> getTypeClasses() {
         return Util.asSet(InDoubtTxInfoImpl.class);
      }
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      InDoubtTxInfoImpl that = (InDoubtTxInfoImpl) o;
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
