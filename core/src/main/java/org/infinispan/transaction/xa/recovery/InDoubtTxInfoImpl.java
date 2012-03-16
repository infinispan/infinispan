/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */

package org.infinispan.transaction.xa.recovery;

import org.infinispan.marshall.AbstractExternalizer;
import org.infinispan.marshall.Ids;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.Util;

import javax.transaction.xa.Xid;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
* @author Mircea Markus
* @since 5.0
*/
public class InDoubtTxInfoImpl implements RecoveryManager.InDoubtTxInfo {
   private Xid xid;
   private Long internalId;
   private Set<Integer> status;
   private transient Set<Address> owners = new HashSet<Address>();
   private transient boolean isLocal;

   public InDoubtTxInfoImpl(Xid xid, Long internalId, Integer status) {
      this.xid = xid;
      this.internalId = internalId;
      this.status = Collections.singleton(status);
   }

   public InDoubtTxInfoImpl(Xid xid, long internalId, Set<Integer> status) {
      this.xid = xid;
      this.internalId = internalId;
      this.status = new HashSet<Integer>(status);
   }

   public InDoubtTxInfoImpl(Xid xid, long internalId) {
      this(xid, internalId, Collections.<Integer>emptySet());
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
         output.writeObject(inDoubtTxInfoImpl.status);
      }

      @Override
      public InDoubtTxInfoImpl readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return new InDoubtTxInfoImpl((Xid) input.readObject(), input.readLong(), (Set<Integer>)input.readObject());
      }

      @Override
      public Integer getId() {
         return Ids.IN_DOUBT_TX_INFO;
      }

      @Override
      public Set<Class<? extends InDoubtTxInfoImpl>> getTypeClasses() {
         return Util.<Class<? extends InDoubtTxInfoImpl>>asSet(InDoubtTxInfoImpl.class);
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
