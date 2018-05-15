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
   //first byte is the first byte of branch id.
   //The rest of the array is the GlobalId + BranchId
   private final byte[] rawId;
   private transient int cachedHashCode;

   protected XidImpl(int formatId, byte[] globalTransactionId, byte[] branchQualifier) {
      this.formatId = formatId;
      rawId = new byte[globalTransactionId.length + branchQualifier.length + 1];
      rawId[0] = (byte) ((globalTransactionId.length + 1) & 0xFF); //first byte of branch id.
      System.arraycopy(globalTransactionId, 0, rawId, 1, globalTransactionId.length);
      System.arraycopy(branchQualifier, 0, rawId, globalTransactionId.length + 1, branchQualifier.length);
   }

   private XidImpl(int formatId, byte[] rawId) {
      this.formatId = formatId;
      this.rawId = rawId;
   }

   private static void validateArray(String name, byte[] array, int maxLength) {
      if (array.length < 1 || array.length > maxLength) {
         throw new IllegalArgumentException(name + " length should be between 1 and " + maxLength);
      }
   }

   public static XidImpl create(int formatId, byte[] globalTransactionId, byte[] branchQualifier) {
      validateArray("GlobalTransactionId", globalTransactionId, MAXGTRIDSIZE);
      validateArray("BranchQualifier", branchQualifier, MAXBQUALSIZE);
      return new XidImpl(formatId, globalTransactionId, branchQualifier);
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
         return new XidImpl(externalXid.getFormatId(), externalXid.getGlobalTransactionId(),
               externalXid.getBranchQualifier());
      }
   }

   public static String printXid(int formatId, byte[] globalTransaction, byte[] branchQualifier) {
      return "Xid{formatId=" + formatId +
            ", globalTransactionId=" + Util.toHexString(globalTransaction) +
            ",branchQualifier=" + Util.toHexString(branchQualifier) +
            "}";
   }

   @Override
   public int getFormatId() {
      return formatId;
   }

   @Override
   public byte[] getGlobalTransactionId() {
      return Arrays.copyOfRange(rawId, globalIdOffset(), branchQualifierOffset());
   }

   @Override
   public byte[] getBranchQualifier() {
      return Arrays.copyOfRange(rawId, branchQualifierOffset(), rawId.length);
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

      if (o instanceof Xid) {
         if (formatId != ((Xid) o).getFormatId()) {
            return false;
         }
         int firstByteOfBranch = rawId[0] & 0xFF;
         return arraysEquals(((Xid) o).getGlobalTransactionId(), 1, firstByteOfBranch, firstByteOfBranch - 1) &&
               arraysEquals(((Xid) o).getBranchQualifier(), firstByteOfBranch, rawId.length,
                     rawId.length - firstByteOfBranch);
      }
      return false;
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
      int firstByteOfBranch = rawId[0] & 0xFF;
      return "Xid{formatId=" + formatId +
            ", globalTransactionId=" + Util.toHexString(rawId, 1, firstByteOfBranch) +
            ",branchQualifier=" + Util.toHexString(rawId, firstByteOfBranch, rawId.length) +
            "}";
   }

   protected int globalIdOffset() {
      return 1;
   }

   protected int globalIdLength() {
      return branchQualifierOffset() - 1; //we need to remove the control byte
   }

   protected int branchQualifierOffset() {
      return (rawId[0] & 0xFF);
   }

   protected int branchQualifierLength() {
      return rawId.length - branchQualifierOffset();
   }

   protected byte[] rawData() {
      return rawId;
   }

   private boolean arraysEquals(byte[] other, int start, int end, int length) {
      if (other.length != length) {
         return false;
      }
      for (int i = start, j = 0; i < end; ++i, ++j) {
         if (rawId[i] != other[j]) {
            return false;
         }
      }
      return true;
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
