package org.infinispan.commons.tx;

import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.nio.ByteBuffer;
import java.util.Arrays;

import javax.transaction.xa.Xid;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.commons.util.Util;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * A {@link Xid} implementation.
 * <p>
 * If need to be serialized, use the methods {@link #writeTo(ObjectOutput, XidImpl)} and {@link #readFrom(ObjectInput)}
 * or the {@link AdvancedExternalizer} in {@link #EXTERNALIZER}.
 *
 * @author Pedro Ruivo
 * @since 9.1
 */
@ProtoTypeId(ProtoStreamTypeIds.XID_IMPL)
public class XidImpl implements Xid {

   @ProtoField(1)
   final int formatId;
   //first byte is the first byte of branch id.
   //The rest of the array is the GlobalId + BranchId
   @ProtoField(2)
   final byte[] rawId;

   private transient int cachedHashCode;

   protected XidImpl(int formatId, byte[] globalTransactionId, byte[] branchQualifier) {
      this.formatId = formatId;
      rawId = new byte[globalTransactionId.length + branchQualifier.length + 1];
      rawId[0] = (byte) ((globalTransactionId.length + 1) & 0xFF); //first byte of branch id.
      System.arraycopy(globalTransactionId, 0, rawId, 1, globalTransactionId.length);
      System.arraycopy(branchQualifier, 0, rawId, globalTransactionId.length + 1, branchQualifier.length);
   }

   @ProtoFactory
   XidImpl(int formatId, byte[] rawId) {
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

   public static XidImpl copy(Xid externalXid) {
      return externalXid instanceof XidImpl ?
            (XidImpl) externalXid :
            new XidImpl(externalXid.getFormatId(), externalXid.getGlobalTransactionId(),
                  externalXid.getBranchQualifier());
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

   public ByteBuffer getGlobalTransactionIdAsByteBuffer() {
      return ByteBuffer.wrap(rawId, globalIdOffset(), globalIdLength());
   }

   @Override
   public byte[] getBranchQualifier() {
      return Arrays.copyOfRange(rawId, branchQualifierOffset(), rawId.length);
   }

   public ByteBuffer getBranchQualifierAsByteBuffer() {
      return ByteBuffer.wrap(rawId, branchQualifierOffset(), branchQualifierLength());
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
}
