package org.infinispan.marshall.persistence.impl;

import java.io.IOException;

import org.infinispan.commons.marshall.WrappedByteArray;
import org.infinispan.protostream.MessageMarshaller;

final class WrappedByteArrayMarshaller implements MessageMarshaller<WrappedByteArray> {

   @Override
   public Class<WrappedByteArray> getJavaClass() {
      return WrappedByteArray.class;
   }

   @Override
   public String getTypeName() {
      return "persistence.WrappedByteArray";
   }

   @Override
   public WrappedByteArray readFrom(ProtoStreamReader reader) throws IOException {
      byte[] bytes = reader.readBytes("wrappedBytes");
      return new WrappedByteArray(bytes);
   }

   @Override
   public void writeTo(ProtoStreamWriter writer, WrappedByteArray wrappedByteArray) throws IOException {
      writer.writeBytes("wrappedBytes", wrappedByteArray.getBytes());
   }
}
