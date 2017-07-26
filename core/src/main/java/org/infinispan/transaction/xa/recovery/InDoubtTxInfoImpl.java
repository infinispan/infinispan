package org.infinispan.transaction.xa.recovery;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import javax.transaction.xa.Xid;

import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.util.SmallIntSet;
import org.infinispan.commons.util.Util;
import org.infinispan.marshall.core.Ids;
import org.infinispan.remoting.transport.Address;

/**
* @author Mircea Markus
* @since 5.0
*/
public class InDoubtTxInfoImpl implements RecoveryManager.InDoubtTxInfo {
   private Xid xid;
   private Long internalId;
   private SmallIntSet status;
   private transient Set<Address> owners = new HashSet<>();
   private transient boolean isLocal;

   public InDoubtTxInfoImpl(Xid xid, Long internalId, Integer status) {
      this.xid = xid;
      this.internalId = internalId;
      this.status = new SmallIntSet(status);
      this.status.set(status);
   }

   public InDoubtTxInfoImpl(Xid xid, long internalId, Set<Integer> status) {
      this.xid = xid;
      this.internalId = internalId;
      this.status = SmallIntSet.from(status);
   }

   public InDoubtTxInfoImpl(Xid xid, long internalId) {
      this(xid, internalId, Collections.emptySet());
   }

   @Override
   public Xid getXid() {
      return xid;
   }

   @Override
   public Long getInternalId() {
      return internalId;
   }

   @Override
   public Set<Integer> getStatus() {
      return status;
   }

   @Override
   public Set<Address> getOwners() {
      return owners;
   }

   public void addStatus(Set<Integer> statusSet) {
      status.addAll(statusSet);
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
         SmallIntSet.writeTo(output, inDoubtTxInfoImpl.status);
      }

      @Override
      public InDoubtTxInfoImpl readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return new InDoubtTxInfoImpl((Xid) input.readObject(), input.readLong(), SmallIntSet.readFrom(input));
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

      if (isLocal != that.isLocal) return false;
      if (internalId != null ? !internalId.equals(that.internalId) : that.internalId != null) return false;
      if (owners != null ? !owners.equals(that.owners) : that.owners != null) return false;
      if (status != null ? !status.equals(that.status) : that.status != null) return false;
      if (xid != null ? !xid.equals(that.xid) : that.xid != null) return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = xid != null ? xid.hashCode() : 0;
      result = 31 * result + (internalId != null ? internalId.hashCode() : 0);
      result = 31 * result + (status != null ? status.hashCode() : 0);
      result = 31 * result + (owners != null ? owners.hashCode() : 0);
      result = 31 * result + (isLocal ? 1 : 0);
      return result;
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
