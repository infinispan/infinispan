package org.infinispan.commons.marshall;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;
import java.util.Collections;
import java.util.Set;

import org.infinispan.commons.util.Util;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * Simple wrapper around a byte[] to provide equals and hashCode semantics
 * @author wburns
 * @since 9.0
 */
@ProtoTypeId(ProtoStreamTypeIds.WRAPPED_BYTE_ARRAY)
public class WrappedByteArray implements WrappedBytes {
   public static final WrappedByteArray EMPTY_BYTES = new WrappedByteArray(Util.EMPTY_BYTE_ARRAY);
   private final byte[] bytes;
   private transient int hashCode;
   private transient boolean initializedHashCode;

   @ProtoFactory
   public WrappedByteArray(byte[] bytes) {
      this.bytes = bytes;
   }

   public WrappedByteArray(byte[] bytes, int hashCode) {
      this.bytes = bytes;
      assert hashCode == Arrays.hashCode(bytes) : "HashCode " + hashCode + " doesn't match " + Arrays.hashCode(bytes);
      this.hashCode = hashCode;
      this.initializedHashCode = true;
   }

   @Override
   @ProtoField(number = 1)
   public byte[] getBytes() {
      return bytes;
   }

   @Override
   public int backArrayOffset() {
      return 0;
   }

   @Override
   public int getLength() {
      return bytes.length;
   }

   @Override
   public byte getByte(int offset) {
      return bytes[offset];
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null) return false;
      Class<?> oClass = o.getClass();
      if (getClass() != oClass) {
         return WrappedBytes.class.isAssignableFrom(oClass) && equalsWrappedBytes((WrappedBytes) o);
      }

      WrappedByteArray that = (WrappedByteArray) o;

      return Arrays.equals(bytes, that.bytes);
   }

   public boolean equalsWrappedBytes(WrappedBytes other) {
      int length = getLength();
      if (other.getLength() != length) return false;
      if (other.hashCode() != hashCode()) return false;
      for (int i = 0; i < length; ++i) {
         if (getByte(i) != other.getByte(i)) return false;
      }
      return true;
   }

   @Override
   public int hashCode() {
      if (!initializedHashCode) {
         this.hashCode = Arrays.hashCode(bytes);
         initializedHashCode = true;
      }
      return hashCode;
   }

   @Override
   public String toString() {
      return "WrappedByteArray{" +
            "bytes=" + Util.hexDump(bytes) +
            ", hashCode=" + hashCode +
            '}';
   }

   public static final class Externalizer extends AbstractExternalizer<WrappedByteArray> {

      @Override
      public Set<Class<? extends WrappedByteArray>> getTypeClasses() {
         return Collections.singleton(WrappedByteArray.class);
      }

      @Override
      public Integer getId() {
         return Ids.WRAPPED_BYTE_ARRAY;
      }

      @Override
      public void writeObject(ObjectOutput output, WrappedByteArray object) throws IOException {
         MarshallUtil.marshallByteArray(object.bytes, output);
         if (object.initializedHashCode) {
            output.writeBoolean(true);
            output.writeInt(object.hashCode);
         } else {
            output.writeBoolean(false);
         }
      }

      @Override
      public WrappedByteArray readObject(ObjectInput input) throws IOException {
         byte[] bytes = MarshallUtil.unmarshallByteArray(input);
         boolean hasHashCode = input.readBoolean();
         if (hasHashCode) {
            return new WrappedByteArray(bytes, input.readInt());
         } else {
            return new WrappedByteArray(bytes);
         }
      }
   }
}
