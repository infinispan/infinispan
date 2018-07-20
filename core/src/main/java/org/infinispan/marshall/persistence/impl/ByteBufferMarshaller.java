package org.infinispan.marshall.persistence.impl;

import java.io.IOException;

import org.infinispan.commons.io.ByteBufferImpl;
import org.infinispan.protostream.MessageMarshaller;

final class ByteBufferMarshaller implements MessageMarshaller<ByteBufferImpl> {
   @Override
   public ByteBufferImpl readFrom(ProtoStreamReader reader) throws IOException {
      byte[] bytes = reader.readBytes("buffer");
      return new ByteBufferImpl(bytes);
   }

   @Override
   public void writeTo(ProtoStreamWriter writer, ByteBufferImpl byteBuffer) throws IOException {
      writer.writeBytes("buffer", byteBuffer.getBuf());
   }

   @Override
   public Class<? extends ByteBufferImpl> getJavaClass() {
      return ByteBufferImpl.class;
   }

   @Override
   public String getTypeName() {
      return "persistence.ByteBuffer";
   }
}
