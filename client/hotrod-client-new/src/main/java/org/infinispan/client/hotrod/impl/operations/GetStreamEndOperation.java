package org.infinispan.client.hotrod.impl.operations;

import org.infinispan.client.hotrod.impl.InternalRemoteCache;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.client.hotrod.impl.transport.netty.HeaderDecoder;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

public class GetStreamEndOperation extends AbstractCacheOperation<Void> {
   private final int id;

   protected GetStreamEndOperation(InternalRemoteCache<?, ?> internalRemoteCache, int id) {
      super(internalRemoteCache);
      this.id = id;
   }

   @Override
   public void writeOperationRequest(Channel channel, ByteBuf buf, Codec codec) {
      buf.writeInt(id);
   }

   @Override
   public Void createResponse(ByteBuf buf, short status, HeaderDecoder decoder, Codec codec, CacheUnmarshaller unmarshaller) {
      return null;
   }

   @Override
   public short requestOpCode() {
      return HotRodConstants.END_GET_STREAM_REQUEST;
   }

   @Override
   public short responseOpCode() {
      return HotRodConstants.END_GET_STREAM_RESPONSE;
   }

   @Override
   public boolean supportRetry() {
      return false;
   }
}
