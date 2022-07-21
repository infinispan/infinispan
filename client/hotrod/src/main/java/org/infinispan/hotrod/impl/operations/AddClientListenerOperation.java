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

public class AddClientListenerOperation<K, V> extends ClientListenerOperation<K, V> {

   private final ClientCacheListenerOptions.Impl cacheListenerOptions;
   private final RemoteCache<?, ?> remoteCache;

   protected AddClientListenerOperation(OperationContext operationContext, ClientCacheListenerOptions.Impl cacheListenerOptions,
         DataFormat dataFormat, FlowableProcessor<CacheEntryEvent<K, V>> processor, RemoteCache<?, ?> remoteCache) {
      this(operationContext, cacheListenerOptions, generateListenerId(), dataFormat, processor, remoteCache);
   }

   private AddClientListenerOperation(OperationContext operationContext, ClientCacheListenerOptions.Impl cacheListenerOptions,
         byte[] listenerId, DataFormat dataFormat, FlowableProcessor<CacheEntryEvent<K, V>> processor, RemoteCache<?, ?> remoteCache) {
      super(operationContext, ADD_CLIENT_LISTENER_REQUEST, ADD_CLIENT_LISTENER_RESPONSE, cacheListenerOptions, listenerId, dataFormat, processor);
      this.cacheListenerOptions = cacheListenerOptions;
      this.remoteCache = remoteCache;
   }

   public AddClientListenerOperation copy() {
      return new AddClientListenerOperation(operationContext, cacheListenerOptions, listenerId, dataFormat, processor, remoteCache);
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
      codec.writeClientListenerParams(buf, cacheListenerOptions);
      codec.writeClientListenerInterests(buf, cacheListenerOptions.eventTypes());
      channel.writeAndFlush(buf);
   }
}
