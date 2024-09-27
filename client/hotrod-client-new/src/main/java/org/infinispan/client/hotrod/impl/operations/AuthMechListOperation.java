package org.infinispan.client.hotrod.impl.operations;

import java.util.ArrayList;
import java.util.List;

import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.client.hotrod.impl.transport.netty.ByteBufUtil;
import org.infinispan.client.hotrod.impl.transport.netty.HeaderDecoder;

import io.netty.buffer.ByteBuf;

public class AuthMechListOperation extends AbstractNoCacheHotRodOperation<List<String>> {
   private int mechCount = -1;
   private List<String> result;

   @Override
   public List<String> createResponse(ByteBuf buf, short status, HeaderDecoder decoder, Codec codec, CacheUnmarshaller unmarshaller) {
      if (mechCount < 0) {
         mechCount = ByteBufUtil.readVInt(buf);
         result = new ArrayList<>(mechCount);
         decoder.checkpoint();
      }
      while (result.size() < mechCount) {
         result.add(ByteBufUtil.readString(buf));
         decoder.checkpoint();
      }
      return result;
   }

   @Override
   public short requestOpCode() {
      return HotRodConstants.AUTH_MECH_LIST_REQUEST;
   }

   @Override
   public short responseOpCode() {
      return HotRodConstants.AUTH_MECH_LIST_RESPONSE;
   }
}
