package org.infinispan.io;

import java.io.IOException;

import net.jcip.annotations.ThreadSafe;

/**
 * A byte stream that is immutable.  Bytes are captured during construction and cannot be written to thereafter.
 *
 * @author Manik Surtani
 * @since 5.1
 */
@ThreadSafe
public final class ImmutableMarshalledValueByteStream extends MarshalledValueByteStream {
   private final byte[] bytes;

   public ImmutableMarshalledValueByteStream(byte[] bytes) {
      this.bytes = bytes;
   }

   @Override
   public int size() {
      return bytes.length;
   }

   @Override
   public byte[] getRaw() {
      return bytes;
   };

   @Override
   public void write(int b) throws IOException {
      throw new UnsupportedOperationException("Immutable");
   }

   @Override
   public boolean equals(Object thatObject) {
      if (thatObject instanceof MarshalledValueByteStream) {
         MarshalledValueByteStream that = (MarshalledValueByteStream) thatObject;
         if (this == that) return true;
         byte[] thoseBytes = that.getRaw();
         if (this.bytes == thoseBytes) return true;
         if (this.size() != that.size()) return false;
         for (int i=0; i<bytes.length; i++) {
            if (this.bytes[i] != thoseBytes[i]) return false;
         }
         return true;
      } else {
         return false;
      }
   }

   @Override
   public int hashCode() {
      //Implementation would either be slow or not consistent with the equals definition
      //just avoid needing the hashCode:
      throw new UnsupportedOperationException();
   }

}
