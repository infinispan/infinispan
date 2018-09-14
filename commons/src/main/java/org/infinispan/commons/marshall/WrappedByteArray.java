package org.infinispan.commons.marshall;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;
import java.util.Collections;
import java.util.Set;

import org.infinispan.commons.util.Util;

/**
 * Simple wrapper around a byte[] to provide equals and hashCode semantics.
 *
 * @author wburns
 * @since 9.0
 */
public final class WrappedByteArray implements WrappedBytes {

   /**
    * My precious bytes.
    */
   private final byte[] bytes;

   /**
    * A lazily computed hashCode. The value 0 is used as a marker to indicate it has not been computed yet, but it is
    * also a valid hashCode value so it can lead to re-computation on each {@link #hashCode()} invocation in this
    * singular unfortunate case. This is still more efficient than having a second 'initializedHashCode' flag because
    * that would lead to concurrency issues that require use of synchronization / volatile for these two fields.
    */
   private int hashCode = 0;

   public WrappedByteArray(byte[] bytes) {
      this.bytes = bytes;
   }

   public WrappedByteArray(byte[] bytes, int hashCode) {
      this.bytes = bytes;
      assert hashCode == Arrays.hashCode(bytes) : "HashCode " + hashCode + " doesn't match " + Arrays.hashCode(bytes);
      this.hashCode = hashCode;
   }

   @Override
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
      if (getClass() == oClass) {
         WrappedByteArray that = (WrappedByteArray) o;
         return Arrays.equals(bytes, that.bytes);
      }

      // fallback to alternative comparison method for WrappedBytes
      return WrappedBytes.class.isAssignableFrom(oClass) && equalsWrappedBytes((WrappedBytes) o);
   }

   /**
    * {@inheritDoc}
    * <p>
    * <b>WARNING:</b> This implementation takes a shortcut and compares the {@link Object#hashCode}s before performing
    * a byte to byte comparison. This will speedup comparison in our particular scenario but can be broken easily by a
    * {@link Object#hashCode} implementation in a WrappedBytes that takes into account other instance fields besides the
    * actual wrapped byte[].
    */
   @Override
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
      if (hashCode == 0) {
         hashCode = Arrays.hashCode(bytes);
      }
      return hashCode;
   }

   @Override
   public String toString() {
      return "WrappedByteArray{" +
            "bytes=" + Util.printArray(bytes) +
            ", hashCode=" + hashCode() +
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
         output.writeInt(object.hashCode());
      }

      @Override
      public WrappedByteArray readObject(ObjectInput input) throws IOException {
         byte[] bytes = MarshallUtil.unmarshallByteArray(input);
         int hashCode = input.readInt();
         return new WrappedByteArray(bytes, hashCode);
      }
   }
}
