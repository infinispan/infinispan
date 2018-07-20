package org.infinispan.transaction.xa.recovery;

import java.io.IOException;
import java.util.Arrays;
import java.util.Set;

import javax.transaction.xa.Xid;

import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.marshall.UserObjectInput;
import org.infinispan.commons.marshall.UserObjectOutput;
import org.infinispan.commons.util.Util;
import org.infinispan.marshall.core.Ids;

import net.jcip.annotations.Immutable;

/**
 * This xid implementation is needed because {@link javax.transaction.xa.Xid} is not {@link java.io.Serializable} and
 * we need to serialize it and send it over the network. As the KTA spec does not enforce in anyway the equals and hashcode methods on Xid
 * TM providers are expected to be able to cope with this Xid class when returned from XAResource's methods.
 *
 * @author Mircea.Markus@jboss.com
 * @since 5.0
 * @deprecated use {@link org.infinispan.commons.tx.XidImpl} instead.
 */
@Immutable
@Deprecated
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

   @Override
   public byte[] getBranchQualifier() {
      return branchQualifier;
   }

   @Override
   public byte[] getGlobalTransactionId() {
      return globalTransactionId;
   }

   @Override
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
      public void writeObject(UserObjectOutput output, SerializableXid object) throws IOException {
         output.writeObject(object.getBranchQualifier());
         output.writeObject(object.getGlobalTransactionId());
         output.writeInt(object.getFormatId());
      }

      @Override
      public SerializableXid readObject(UserObjectInput input) throws IOException, ClassNotFoundException {
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
