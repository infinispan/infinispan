package org.infinispan.hotrod.impl.operations;

import org.infinispan.api.common.events.cache.CacheEntryEvent;
import org.infinispan.hotrod.api.ClientCacheListenerOptions;
import org.infinispan.hotrod.event.impl.ClientEventDispatcher;
import org.infinispan.hotrod.impl.DataFormat;
import org.infinispan.hotrod.impl.cache.RemoteCache;
import org.infinispan.hotrod.impl.protocol.Codec;
import org.infinispan.hotrod.impl.transport.netty.ByteBufUtil;
import org.infinispan.hotrod.impl.transport.netty.HeaderDecoder;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.reactivex.rxjava3.processors.FlowableProcessor;

/**
 *
 */
public class AddBloomNearCacheClientListenerOperation<K, V> extends ClientListenerOperation<K, V> {

   private final int bloomFilterBits;
   private final RemoteCache<?, ?> remoteCache;

   protected AddBloomNearCacheClientListenerOperation(OperationContext operationContext, ClientCacheListenerOptions.Impl listenerOptions,
         int bloomFilterBits, DataFormat dataFormat, FlowableProcessor<CacheEntryEvent<K, V>> flowableProcessor, RemoteCache<?, ?> remoteCache) {
      this(operationContext, listenerOptions, generateListenerId(), dataFormat, bloomFilterBits, flowableProcessor, remoteCache);
   }

   private AddBloomNearCacheClientListenerOperation(OperationContext operationContext,
         ClientCacheListenerOptions.Impl listenerOptions,
         byte[] listenerId, DataFormat dataFormat, int bloomFilterBits,
         FlowableProcessor<CacheEntryEvent<K, V>> flowableProcessor, RemoteCache<?, ?> remoteCache) {
      super(operationContext, ADD_BLOOM_FILTER_NEAR_CACHE_LISTENER_REQUEST, ADD_BLOOM_FILTER_NEAR_CACHE_LISTENER_RESPONSE,
            listenerOptions, listenerId, dataFormat, flowableProcessor);
      this.bloomFilterBits = bloomFilterBits;
      this.remoteCache = remoteCache;
   }

   public AddBloomNearCacheClientListenerOperation copy() {
      return new AddBloomNearCacheClientListenerOperation(operationContext, listenerOptions, listenerId, dataFormat, bloomFilterBits, processor, remoteCache);
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
