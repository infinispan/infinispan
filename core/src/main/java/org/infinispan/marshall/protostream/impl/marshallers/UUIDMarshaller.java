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
      long mostSigBits = reader.readLong("mostSigBits");
      long leastSigBits = reader.readLong("leastSigBits");
      return new UUID(mostSigBits, leastSigBits);
   }

   @Override
   public void writeTo(ProtoStreamWriter writer, UUID uuid) throws IOException {
      writer.writeLong("mostSigBits", uuid.getMostSignificantBits());
      writer.writeLong("leastSigBits", uuid.getLeastSignificantBits());
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
