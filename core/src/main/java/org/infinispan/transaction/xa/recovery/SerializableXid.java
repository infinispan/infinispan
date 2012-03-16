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

import net.jcip.annotations.Immutable;
import org.infinispan.marshall.AbstractExternalizer;
import org.infinispan.marshall.Ids;
import org.infinispan.util.Util;

import javax.transaction.xa.Xid;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;
import java.util.Set;

/**
 * This xid implementation is needed because {@link javax.transaction.xa.Xid} is not {@link java.io.Serializable} and
 * we need to serialize it and send it over the network. As the KTA spec does not enforce in anyway the equals and hashcode methods on Xid
 * TM providers are expected to be able to cope with this Xid class when returned from XAResource's methods.
 *
 * @author Mircea.Markus@jboss.com
 * @since 5.0
 */
@Immutable
public class SerializableXid implements Xid {

   private final byte[] branchQualifier;
   private final byte[] globalTransactionId;
   private final int formatId;

   public SerializableXid(byte[] branchQualifier, byte[] globalTransactionId, int formantId) {
      this.branchQualifier = branchQualifier;
      this.globalTransactionId = globalTransactionId;
      this.formatId = formantId;
   }

   public SerializableXid(Xid xid) {
      this(xid.getBranchQualifier(), xid.getGlobalTransactionId(), xid.getFormatId());
   }

   public byte[] getBranchQualifier() {
      return branchQualifier;
   }

   public byte[] getGlobalTransactionId() {
      return globalTransactionId;
   }

   public int getFormatId() {
      return formatId;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || !(o instanceof Xid)) return false;

      Xid xid = (Xid) o;

      if (formatId != xid.getFormatId()) return false;
      if (!Arrays.equals(branchQualifier, xid.getBranchQualifier())) return false;
      if (!Arrays.equals(globalTransactionId, xid.getGlobalTransactionId())) return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = branchQualifier != null ? Arrays.hashCode(branchQualifier) : 0;
      result = 31 * result + (globalTransactionId != null ? Arrays.hashCode(globalTransactionId) : 0);
      result = 31 * result + formatId;
      return result;
   }

   @Override
   public String toString() {
      //taken from com.arjuna.ats.jta.xa.XID
      StringBuilder stringBuilder = new StringBuilder();
      stringBuilder.append("< ");
      stringBuilder.append(formatId);
      stringBuilder.append(", ");
      stringBuilder.append(globalTransactionId.length);
      stringBuilder.append(", ");
      stringBuilder.append(branchQualifier.length);
      stringBuilder.append(", ");

      for (byte aGlobalTransactionId : globalTransactionId) {
         stringBuilder.append(aGlobalTransactionId);
      }
      stringBuilder.append(", ");
      for (byte aBranchQualifier : branchQualifier) {
         stringBuilder.append(aBranchQualifier);
      }
      stringBuilder.append(" >");
      return stringBuilder.toString();
   }

   public static class XidExternalizer extends AbstractExternalizer<SerializableXid> {

      @Override
      public void writeObject(ObjectOutput output, SerializableXid object) throws IOException {
         output.writeObject(object.getBranchQualifier());
         output.writeObject(object.getGlobalTransactionId());
         output.writeInt(object.getFormatId());
      }

      @Override
      public SerializableXid readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         byte[] bq = (byte[]) input.readObject();
         byte[] gtId = (byte[]) input.readObject();
         int type = input.readInt();
         return new SerializableXid(bq, gtId, type);
      }

      @Override
      public Set<Class<? extends SerializableXid>> getTypeClasses() {
         return Util.<Class<? extends SerializableXid>>asSet(SerializableXid.class);
      }

      @Override
      public Integer getId() {
         return Ids.XID;
      }
   }
}
