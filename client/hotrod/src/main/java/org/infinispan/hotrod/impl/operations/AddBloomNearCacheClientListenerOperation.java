package org.infinispan.hotrod.impl.operations;

import org.infinispan.api.common.CacheOptions;
import org.infinispan.hotrod.event.impl.ClientEventDispatcher;
import org.infinispan.hotrod.impl.DataFormat;
import org.infinispan.hotrod.impl.cache.RemoteCache;
import org.infinispan.hotrod.impl.protocol.Codec;
import org.infinispan.hotrod.impl.transport.netty.ByteBufUtil;
import org.infinispan.hotrod.impl.transport.netty.HeaderDecoder;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

/**
 *
 */
public class AddBloomNearCacheClientListenerOperation extends ClientListenerOperation {

   private final int bloomFilterBits;
   private final RemoteCache<?, ?> remoteCache;

   protected AddBloomNearCacheClientListenerOperation(OperationContext operationContext, CacheOptions options,
                                                      Object listener,
                                                      DataFormat dataFormat,
                                                      int bloomFilterBits, RemoteCache<?, ?> remoteCache) {
      this(operationContext, options, generateListenerId(), listener, dataFormat, bloomFilterBits, remoteCache);
   }

   private AddBloomNearCacheClientListenerOperation(OperationContext operationContext,
                                                    CacheOptions options,
                                                    byte[] listenerId, Object listener,
                                                    DataFormat dataFormat,
                                                    int bloomFilterBits, RemoteCache<?, ?> remoteCache) {
      super(operationContext, ADD_BLOOM_FILTER_NEAR_CACHE_LISTENER_REQUEST, ADD_BLOOM_FILTER_NEAR_CACHE_LISTENER_RESPONSE,
            options, listenerId, dataFormat, listener);
      this.bloomFilterBits = bloomFilterBits;
      this.remoteCache = remoteCache;
   }

   public AddBloomNearCacheClientListenerOperation copy() {
      return new AddBloomNearCacheClientListenerOperation(operationContext, options, listenerId, listener, dataFormat(), bloomFilterBits, remoteCache);
   }

   @Override
   protected void actualExecute(Channel channel) {
      channel.pipeline().get(HeaderDecoder.class).registerOperation(channel, this);

      operationContext.getListenerNotifier().addDispatcher(ClientEventDispatcher.create(this,
            address, () -> cleanup(channel), remoteCache));

      ByteBuf buf = channel.alloc().buffer();

      Codec codec = operationContext.getCodec();
      codec.writeHeader(buf, header);
      ByteBufUtil.writeArray(buf, listenerId);
      codec.writeBloomFilter(buf, bloomFilterBits);
      channel.writeAndFlush(buf);
   }
}
