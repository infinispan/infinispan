package org.infinispan.client.hotrod.impl.operations;

import org.infinispan.client.hotrod.impl.InternalRemoteCache;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.transport.netty.ByteBufUtil;
import org.infinispan.client.hotrod.impl.transport.netty.HeaderDecoder;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

public class AddPrimaryNearCacheClientListenerOperation extends ClientListenerOperation {

   private final int bloomFilterBits;

   public AddPrimaryNearCacheClientListenerOperation(InternalRemoteCache<?, ?> remoteCache, Object listener,
                                                     int bloomFilterBits) {
      super(remoteCache, listener, generateListenerId());
      this.bloomFilterBits = bloomFilterBits;
   }

   public AddPrimaryNearCacheClientListenerOperation(InternalRemoteCache<?, ?> remoteCache, Object listener,
                                                     byte[] listenerId, int bloomFilterBits) {
      super(remoteCache, listener, listenerId);
      this.bloomFilterBits = bloomFilterBits;
   }

   @Override
   public void writeOperationRequest(Channel channel, ByteBuf buf, Codec codec) {
      ByteBufUtil.writeArray(buf, listenerId);
      codec.writeBloomFilter(buf, bloomFilterBits);
   }

   @Override
   public Channel createResponse(ByteBuf buf, short status, HeaderDecoder decoder, Codec codec, CacheUnmarshaller unmarshaller) {
      return decoder.getChannel();
   }

   @Override
   public short requestOpCode() {
      return ADD_PRIMARY_NEAR_CACHE_LISTENER_REQUEST;
   }

   @Override
   public short responseOpCode() {
      return ADD_PRIMARY_NEAR_CACHE_LISTENER_RESPONSE;
   }

   @Override
   public ClientListenerOperation copy() {
      return new AddPrimaryNearCacheClientListenerOperation(internalRemoteCache, listener, listenerId, bloomFilterBits);
   }

   public int getBloomFilterBits() {
      return bloomFilterBits;
   }

   @Override
   public boolean supportRetry() {
      return false;
   }
}
