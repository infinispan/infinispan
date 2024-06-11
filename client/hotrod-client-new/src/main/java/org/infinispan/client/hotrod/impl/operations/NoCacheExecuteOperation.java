package org.infinispan.client.hotrod.impl.operations;

import java.util.Map;

import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.client.hotrod.impl.transport.netty.ByteBufUtil;
import org.infinispan.client.hotrod.impl.transport.netty.HeaderDecoder;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

public class NoCacheExecuteOperation extends AbstractNoCacheHotRodOperation<String> {
   private final String taskName;
   private final Map<String, byte[]> marshalledParams;

   public NoCacheExecuteOperation(String taskName, Map<String, byte[]> marshalledParams) {
      this.taskName = taskName;
      this.marshalledParams = marshalledParams;
   }

   @Override
   public void writeOperationRequest(Channel channel, ByteBuf buf, Codec codec) {
      ByteBufUtil.writeString(buf, taskName);
      ByteBufUtil.writeVInt(buf, marshalledParams.size());
      for (Map.Entry<String, byte[]> entry : marshalledParams.entrySet()) {
         ByteBufUtil.writeString(buf, entry.getKey());
         ByteBufUtil.writeArray(buf, entry.getValue());
      }
   }

   @Override
   public String createResponse(ByteBuf buf, short status, HeaderDecoder decoder, Codec codec, CacheUnmarshaller unmarshaller) {
      return ByteBufUtil.readString(buf);
   }

   @Override
   public short requestOpCode() {
      return HotRodConstants.EXEC_REQUEST;
   }

   @Override
   public short responseOpCode() {
      return HotRodConstants.EXEC_RESPONSE;
   }
}
