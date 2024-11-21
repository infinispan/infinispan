package org.infinispan.client.hotrod.impl.operations;

import org.infinispan.client.hotrod.impl.InternalRemoteCache;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.transport.netty.ByteBufUtil;
import org.infinispan.client.hotrod.impl.transport.netty.HeaderDecoder;

import io.netty.buffer.ByteBuf;

public class SizeOperation extends AbstractCacheOperation<Integer> {

   protected SizeOperation(InternalRemoteCache<?, ?> remoteCache) {
      super(remoteCache);
   }

   @Override
   public Integer createResponse(ByteBuf buf, short status, HeaderDecoder decoder, Codec codec, CacheUnmarshaller unmarshaller) {
      return ByteBufUtil.readVInt(buf);
   }

   @Override
   public short requestOpCode() {
      return SIZE_REQUEST;
   }

   @Override
   public short responseOpCode() {
      return SIZE_RESPONSE;
   }
}
