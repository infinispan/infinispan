package org.infinispan.marshall.protostream.impl.marshallers;

import java.io.IOException;
import java.util.UUID;

import org.infinispan.protostream.MessageMarshaller;

public class UUIDMarshaller implements MessageMarshaller<UUID> {

   private final String typeName;

   /**
    * @param typeName so that marshaller can be used in multiple contexts
    */
   public UUIDMarshaller(String typeName) {
      this.typeName = typeName;
   }

   @Override
   public UUID readFrom(ProtoStreamReader reader) throws IOException {
      Long mostSigBits = reader.readLong("mostSigBits");
      Long leastSigBits = reader.readLong("leastSigBits");

      if (mostSigBits == null) {
         mostSigBits = reader.readLong("mostSigBitsFixed");
         leastSigBits = reader.readLong("leastSigBitsFixed");
      }
      return new UUID(mostSigBits, leastSigBits);
   }

   @Override
   public void writeTo(ProtoStreamWriter writer, UUID uuid) throws IOException {
      writer.writeLong("mostSigBitsFixed", uuid.getMostSignificantBits());
      writer.writeLong("leastSigBitsFixed", uuid.getLeastSignificantBits());
   }

   @Override
   public Class<? extends UUID> getJavaClass() {
      return UUID.class;
   }

   @Override
   public String getTypeName() {
      return typeName;
   }
}
