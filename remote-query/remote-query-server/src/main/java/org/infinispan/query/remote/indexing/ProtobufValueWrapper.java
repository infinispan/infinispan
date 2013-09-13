package org.infinispan.query.remote.indexing;

import org.infinispan.commons.io.UnsignedNumeric;
import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.query.remote.ExternalizerIds;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;
import java.util.Collections;
import java.util.Set;

/**
 * This is used to wrap binary values encoded with Protocol Buffers. ProtobufValueWrapperFieldBridge is used as a
 * class bridge to allow indexing of the binary payload.
 *
 * @author anistor@redhat.com
 * @since 6.0
 */
public final class ProtobufValueWrapper {

   // The protobuf encoded payload
   private final byte[] binary;

   private int hashCode = 0;

   public ProtobufValueWrapper(byte[] binary) {
      this.binary = binary;
   }

   public byte[] getBinary() {
      return binary;
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
      return "ProtobufValueWrapper(" + Arrays.toString(binary) + ')';
   }

   public static final class Externalizer extends AbstractExternalizer<ProtobufValueWrapper> {

      @Override
      public void writeObject(ObjectOutput output, ProtobufValueWrapper protobufValueWrapper) throws IOException {
         UnsignedNumeric.writeUnsignedInt(output, protobufValueWrapper.getBinary().length);
         output.write(protobufValueWrapper.getBinary());
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
         return Collections.<Class<? extends ProtobufValueWrapper>>singleton(ProtobufValueWrapper.class);
      }
   }
}
