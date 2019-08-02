package org.infinispan.query.remote.impl.indexing;

import java.util.Arrays;

import org.hibernate.search.spi.IndexedTypeIdentifier;
import org.hibernate.search.spi.impl.PojoIndexedTypeIdentifier;
import org.infinispan.commons.marshall.WrappedBytes;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.descriptors.Descriptor;

/**
 * This is used to wrap binary values encoded with Protocol Buffers. {@link ProtobufValueWrapperFieldBridge} is used as
 * a class bridge to allow indexing of the binary payload.
 *
 * @author anistor@redhat.com
 * @since 6.0
 */
public final class ProtobufValueWrapper implements WrappedBytes {

   public static final IndexedTypeIdentifier INDEXING_TYPE = PojoIndexedTypeIdentifier.convertFromLegacy(ProtobufValueWrapper.class);

   /**
    * Max number of bytes to include in {@link #toString()}.
    */
   private static final int MAX_BYTES_IN_TOSTRING = 40;

   /**
    * The protobuf encoded payload.
    */
   @ProtoField(number = 1)
   byte[] binary;

   /**
    * The lazily computed hashCode. Transient field!
    */
   private transient int hashCode = 0;

   /**
    * The Descriptor of the message (if it's a Message and not a primitive value, or null otherwise). Transient field!
    */
   private transient Descriptor messageDescriptor;

   ProtobufValueWrapper() {}

   public ProtobufValueWrapper(byte[] binary) {
      if (binary == null) {
         throw new IllegalArgumentException("argument cannot be null");
      }
      this.binary = binary;
   }

   /**
    * Gets the internal byte array. Callers should not modify the contents of the array.
    *
    * @return the wrapped byte array
    */
   public byte[] getBinary() {
      return binary;
   }

   /**
    * Returns the Protobuf descriptor of the message type of the payload.
    *
    * @see #setMessageDescriptor
    */
   public Descriptor getMessageDescriptor() {
      return messageDescriptor;
   }

   /**
    * Sets the Protobuf descriptor of the message type of the payload.
    *
    * @see ProtobufValueWrapperSearchWorkCreator#discoverMessageType
    */
   void setMessageDescriptor(Descriptor messageDescriptor) {
      this.messageDescriptor = messageDescriptor;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      ProtobufValueWrapper that = (ProtobufValueWrapper) o;
      return Arrays.equals(binary, that.binary);
   }

   @Override
   public int hashCode() {
      if (hashCode == 0) {
         hashCode = Arrays.hashCode(binary);
      }
      return hashCode;
   }

   @Override
   public String toString() {
      // Render only max 40 bytes
      int len = Math.min(binary.length, MAX_BYTES_IN_TOSTRING);
      StringBuilder sb = new StringBuilder(50 + 3 * len);
      sb.append("ProtobufValueWrapper(length=").append(binary.length).append(", binary=[");
      for (int i = 0; i < len; i++) {
         if (i != 0) {
            sb.append(' ');
         }
         sb.append(String.format("%02X", binary[i] & 0xff));
      }
      if (len < binary.length) {
         sb.append("...");
      }
      sb.append("])");
      return sb.toString();
   }

   @Override
   public byte[] getBytes() {
      return binary;
   }

   @Override
   public int backArrayOffset() {
      return 0;
   }

   @Override
   public int getLength() {
      return binary.length;
   }

   @Override
   public byte getByte(int offset) {
      return binary[offset];
   }

   @Override
   public boolean equalsWrappedBytes(WrappedBytes other) {
      if (other == null) return false;
      if (other.getLength() != binary.length) return false;
      for (int i = 0; i < binary.length; ++i) {
         if (binary[i] != other.getByte(i)) return false;
      }
      return true;
   }
}
