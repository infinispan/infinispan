package org.infinispan.hotrod.impl.operations;

import java.util.EnumSet;

import org.infinispan.api.common.CacheOptions;
import org.infinispan.api.common.events.cache.CacheEntryEventType;
import org.infinispan.hotrod.event.ClientListener;
import org.infinispan.hotrod.event.impl.ClientEventDispatcher;
import org.infinispan.hotrod.impl.DataFormat;
import org.infinispan.hotrod.impl.cache.RemoteCache;
import org.infinispan.hotrod.impl.protocol.Codec;
import org.infinispan.hotrod.impl.transport.netty.ByteBufUtil;
import org.infinispan.hotrod.impl.transport.netty.HeaderDecoder;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

public class AddClientListenerOperation extends ClientListenerOperation {

   private final byte[][] filterFactoryParams;
   private final byte[][] converterFactoryParams;
   private final RemoteCache<?, ?> remoteCache;

   protected AddClientListenerOperation(OperationContext operationContext, CacheOptions options, Object listener,
                                        byte[][] filterFactoryParams, byte[][] converterFactoryParams, DataFormat dataFormat,
                                        RemoteCache<?, ?> remoteCache) {
      this(operationContext, options, generateListenerId(), listener, filterFactoryParams, converterFactoryParams, dataFormat, remoteCache);
   }

   private AddClientListenerOperation(OperationContext operationContext, CacheOptions options,
                                      byte[] listenerId, Object listener,
                                      byte[][] filterFactoryParams, byte[][] converterFactoryParams, DataFormat dataFormat,
                                      RemoteCache<?, ?> remoteCache) {
      super(operationContext, ADD_CLIENT_LISTENER_REQUEST, ADD_CLIENT_LISTENER_RESPONSE, options, listenerId, dataFormat, listener);
      this.filterFactoryParams = filterFactoryParams;
      this.converterFactoryParams = converterFactoryParams;
      this.remoteCache = remoteCache;
   }

   public AddClientListenerOperation copy() {
      return new AddClientListenerOperation(operationContext, options, listenerId, listener, filterFactoryParams,
            converterFactoryParams, dataFormat(), remoteCache);
   }

   @Override
   protected void actualExecute(Channel channel) {
      ClientListener clientListener = null; // FIXME

      channel.pipeline().get(HeaderDecoder.class).registerOperation(channel, this);

      operationContext.getListenerNotifier().addDispatcher(ClientEventDispatcher.create(this,
            address, () -> cleanup(channel), remoteCache));

      ByteBuf buf = channel.alloc().buffer();
      Codec codec = operationContext.getCodec();
      codec.writeHeader(buf, header);
      ByteBufUtil.writeArray(buf, listenerId);
      codec.writeClientListenerParams(buf, clientListener, filterFactoryParams, converterFactoryParams);
      codec.writeClientListenerInterests(buf, EnumSet.noneOf(CacheEntryEventType.class)); //FIXME
      channel.writeAndFlush(buf);
   }
}
