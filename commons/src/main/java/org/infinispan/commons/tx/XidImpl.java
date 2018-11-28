package org.infinispan.commons.tx;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;
import java.util.Collections;
import java.util.Set;

import javax.transaction.xa.Xid;

import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.marshall.Ids;
import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.commons.util.Util;

/**
 * A {@link Xid} implementation.
 * <p>
 * If need to be serialized, use the methods {@link #writeTo(ObjectOutput, XidImpl)} and {@link #readFrom(ObjectInput)}
 * or the {@link AdvancedExternalizer} in {@link #EXTERNALIZER}.
 *
 * @author Pedro Ruivo
 * @since 9.1
 */
public class XidImpl implements Xid {

   public static final AdvancedExternalizer<XidImpl> EXTERNALIZER = new Externalizer();

   private final int formatId;
   private final byte[] rawId;
   private transient int cachedHashCode;

   protected XidImpl(int formatId, byte[] rawId) {
      this.formatId = formatId;
      this.rawId = rawId;
   }

   private static void validateArray(String name, byte[] array, int maxLength) {
      if (array.length < 1 || array.length > maxLength) {
         throw new IllegalArgumentException(name + " length should be between 1 and " + maxLength);
      }
   }

   public static XidImpl create(int formatId, byte[] globalTransactionId) {
      validateArray("GlobalTransactionId", globalTransactionId, MAXGTRIDSIZE);
      return new XidImpl(formatId, globalTransactionId);
   }

   public static void writeTo(ObjectOutput output, XidImpl xid) throws IOException {
      output.writeInt(xid.formatId);
      MarshallUtil.marshallByteArray(xid.rawId, output);
   }

   public static XidImpl readFrom(ObjectInput input) throws IOException {
      return new XidImpl(input.readInt(), MarshallUtil.unmarshallByteArray(input));
   }

   public static XidImpl copy(Xid externalXid) {
      if (externalXid.getClass() == XidImpl.class) {
         return (XidImpl) externalXid;
      } else if (externalXid instanceof XidImpl) {
         return new XidImpl(((XidImpl) externalXid).formatId, ((XidImpl) externalXid).rawId);
      } else {
         return new XidImpl(externalXid.getFormatId(), externalXid.getGlobalTransactionId());
      }
   }

   public static String printXid(int formatId, byte[] globalTransaction) {
      return "Xid{formatId=" + formatId +
            ", globalTransactionId=" + Util.toHexString(globalTransaction) +
            "}";
   }

   @Override
   public int getFormatId() {
      return formatId;
   }

   @Override
   public byte[] getGlobalTransactionId() {
      return rawId.clone();
   }

   @Override
   public byte[] getBranchQualifier() {
      return Util.EMPTY_BYTE_ARRAY;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) {
         return true;
      }
      if (o == null) {
         return false;
      }

      if (o instanceof XidImpl) {
         return formatId == ((XidImpl) o).formatId && Arrays.equals(rawId, ((XidImpl) o).rawId);
      }

      return o instanceof Xid &&
         formatId == ((Xid) o).getFormatId() && ((Xid) o).getBranchQualifier().length == 0 &&
               Arrays.equals(((Xid) o).getGlobalTransactionId(), rawId);
   }

   @Override
   public int hashCode() {
      if (cachedHashCode == 0) {
         int result = formatId;
         //skip the control byte and use only the global transaction and branch qualifier
         for (int i = 1; i < rawId.length; ++i) {
            result = 37 * result + rawId[i];
         }
         cachedHashCode = result;
      }
      return cachedHashCode;
   }

   @Override
   public String toString() {
      return "Xid{formatId=" + formatId +
            ", globalTransactionId=" + Util.toHexString(rawId) +
            "}";
   }

   protected byte[] rawData() {
      return rawId;
   }

   private static class Externalizer implements AdvancedExternalizer<XidImpl> {

      @Override
      public Set<Class<? extends XidImpl>> getTypeClasses() {
         return Collections.singleton(XidImpl.class);
      }

      @Override
      public Integer getId() {
         return Ids.XID_IMPL;
      }

      @Override
      public void writeObject(ObjectOutput output, XidImpl object) throws IOException {
         writeTo(output, object);
      }

      @Override
      public XidImpl readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return readFrom(input);
      }
   }
}
