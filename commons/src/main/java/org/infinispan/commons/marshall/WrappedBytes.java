package org.infinispan.commons.marshall;

/**
 * Interface that describes and object holding onto some bytes
 * @author wburns
 * @since 9.0
 */
public interface WrappedBytes {
   /**
    * The backing array if there is one otherwise null is returned.  Callers should use
    * {@link WrappedBytes#backArrayOffset()} to know where to read the bytes from.  This byte[] should never be modified
    * by the caller
    * @return the backing byte[] if there is one.
    */
   byte[] getBytes();

   /**
    * The offset of where data starts in the backed array.
    * @return -1 if there is no backed array otherwise &ge; 0 if there is backing array
    */
   int backArrayOffset();

   /**
    * The length of the underlying wrapped bytes.  This will always be &ge; 0.
    * @return how many bytes are available from the underlying wrapped implementation
    */
   int getLength();

   /**
    * Retrieves the byte given an offset.  This offset should always be less than {@link WrappedBytes#getLength()}.
    * @param offset the offset of where to find the byte
    * @return the byte at this position
    */
   byte getByte(int offset);

   default boolean equalsWrappedBytes(WrappedBytes other) {
      if (other == null) return false;
      int length = getLength();
      if (other.getLength() != length) return false;
      if (other.hashCode() != hashCode()) return false;
      for (int i = 0; i < length; ++i) {
         if (getByte(i) != other.getByte(i)) return false;
      }
      return true;
   }
}
