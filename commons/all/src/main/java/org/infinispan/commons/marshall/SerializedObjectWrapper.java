package org.infinispan.commons.marshall;

import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * Wrapper that extends WrappedByteArray to signal that the byte array contains a serialized object
 * that must be deserialized when decoding from application/octet-stream media type.
 *
 * @since 16.2
 */
@ProtoTypeId(ProtoStreamTypeIds.SERIALIZED_OBJECT_WRAPPER)
public class SerializedObjectWrapper extends WrappedByteArray {

   @ProtoFactory
   public SerializedObjectWrapper(byte[] bytes) {
      super(bytes);
   }

   public SerializedObjectWrapper(byte[] bytes, int hashCode) {
      super(bytes, hashCode);
   }

   @Override
   public String toString() {
      return "SerializedObjectWrapper[" + getBytes().length + " bytes]";
   }
}
