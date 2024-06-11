package org.infinispan.client.hotrod.impl.operations;

import org.infinispan.client.hotrod.DataFormat;
import org.infinispan.client.hotrod.impl.InternalRemoteCache;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.transport.netty.ByteBufUtil;
import org.infinispan.client.hotrod.impl.transport.netty.HeaderDecoder;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

public class UpdateBloomFilterOperation extends AbstractCacheOperation<Void> {
   private final byte[] bloomBits;

   protected UpdateBloomFilterOperation(InternalRemoteCache<?, ?> remoteCache, byte[] bloomBits) {
      super(remoteCache);
      this.bloomBits = bloomBits;
   }

   @Override
   public void writeOperationRequest(Channel channel, ByteBuf buf, Codec codec) {
      ByteBufUtil.writeArray(buf, bloomBits);
   }

   @Override
   public Void createResponse(ByteBuf buf, short status, HeaderDecoder decoder, Codec codec, CacheUnmarshaller unmarshaller) {
      return null;
   }

   @Override
   public short requestOpCode() {
      return UPDATE_BLOOM_FILTER_REQUEST;
   }

   @Override
   public short responseOpCode() {
      return UPDATE_BLOOM_FILTER_RESPONSE;
   }

   @Override
   public DataFormat getDataFormat() {
      // No data format sent for updating bloom filter as nothing to use format
      return null;
   }
}
