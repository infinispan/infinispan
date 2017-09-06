package org.infinispan.query.remote.impl.indexing;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;
import java.util.Collections;
import java.util.Set;

import org.hibernate.search.spi.IndexedTypeIdentifier;
import org.hibernate.search.spi.impl.PojoIndexedTypeIdentifier;
import org.infinispan.commons.io.UnsignedNumeric;
import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.protostream.descriptors.Descriptor;
import org.infinispan.query.remote.impl.ExternalizerIds;

/**
 * This is used to wrap binary values encoded with Protocol Buffers. {@link ProtobufValueWrapperFieldBridge} is used as
 * a class bridge to allow indexing of the binary payload.
 *
 * @author anistor@redhat.com
 * @since 6.0
 */
public final class ProtobufValueWrapper {

   public static final IndexedTypeIdentifier INDEXING_TYPE = PojoIndexedTypeIdentifier.convertFromLegacy(ProtobufValueWrapper.class);

   // The protobuf encoded payload
   private final byte[] binary;

   private int hashCode = 0;

   // The Descriptor of the message (if it's a Message and not a primitive value). Transient field!
   private Descriptor messageDescriptor;

   public ProtobufValueWrapper(byte[] binary) {
      if (binary == null) {
         throw new IllegalArgumentException("argument cannot be null");
      }
      this.binary = binary;
   }

   public byte[] getBinary() {
      return binary;
   }

   public Descriptor getMessageDescriptor() {
      return messageDescriptor;
   }

   public void setMessageDescriptor(Descriptor messageDescriptor) {
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
      int len = Math.min(binary.length, 40);
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

   public static final class Externalizer extends AbstractExternalizer<ProtobufValueWrapper> {

      @Override
      public void writeObject(ObjectOutput output, ProtobufValueWrapper protobufValueWrapper) throws IOException {
         UnsignedNumeric.writeUnsignedInt(output, protobufValueWrapper.binary.length);
         output.write(protobufValueWrapper.binary);
      }

      @Override
      public ProtobufValueWrapper readObject(ObjectInput input) throws IOException {
         int length = UnsignedNumeric.readUnsignedInt(input);
         byte[] binary = new byte[length];
         input.readFully(binary);
         return new ProtobufValueWrapper(binary);
      }

      @Override
      public Integer getId() {
         return ExternalizerIds.PROTOBUF_VALUE_WRAPPER;
      }

      @Override
      public Set<Class<? extends ProtobufValueWrapper>> getTypeClasses() {
         return Collections.singleton(ProtobufValueWrapper.class);
      }
   }
}
