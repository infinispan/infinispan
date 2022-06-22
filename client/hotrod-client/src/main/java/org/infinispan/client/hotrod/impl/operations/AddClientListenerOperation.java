package org.infinispan.client.hotrod.impl.operations;

import java.util.concurrent.atomic.AtomicReference;

import org.infinispan.client.hotrod.DataFormat;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.annotation.ClientListener;
import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.event.impl.ClientEventDispatcher;
import org.infinispan.client.hotrod.event.impl.ClientListenerNotifier;
import org.infinispan.client.hotrod.impl.ClientTopology;
import org.infinispan.client.hotrod.impl.InternalRemoteCache;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.transport.netty.ByteBufUtil;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelFactory;
import org.infinispan.client.hotrod.impl.transport.netty.HeaderDecoder;
import org.infinispan.client.hotrod.telemetry.impl.TelemetryService;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

/**
 * @author Galder Zamarreño
 */
public class AddClientListenerOperation extends ClientListenerOperation {

   private final byte[][] filterFactoryParams;
   private final byte[][] converterFactoryParams;
   private final InternalRemoteCache<?, ?> remoteCache;
   private final TelemetryService telemetryService;

   protected AddClientListenerOperation(Codec codec, ChannelFactory channelFactory,
                                        String cacheName, AtomicReference<ClientTopology> clientTopology, int flags, Configuration cfg,
                                        ClientListenerNotifier listenerNotifier, Object listener,
                                        byte[][] filterFactoryParams, byte[][] converterFactoryParams, DataFormat dataFormat,
                                        InternalRemoteCache<?, ?> remoteCache, TelemetryService telemetryService) {
      this(codec, channelFactory, cacheName, clientTopology, flags, cfg, generateListenerId(),
            listenerNotifier, listener, filterFactoryParams, converterFactoryParams, dataFormat, remoteCache, telemetryService);
   }

   private AddClientListenerOperation(Codec codec, ChannelFactory channelFactory,
                                      String cacheName, AtomicReference<ClientTopology> clientTopology, int flags, Configuration cfg,
                                      byte[] listenerId, ClientListenerNotifier listenerNotifier, Object listener,
                                      byte[][] filterFactoryParams, byte[][] converterFactoryParams, DataFormat dataFormat,
                                      InternalRemoteCache<?, ?> remoteCache, TelemetryService telemetryService) {
      super(ADD_CLIENT_LISTENER_REQUEST, ADD_CLIENT_LISTENER_RESPONSE, codec, channelFactory,
            RemoteCacheManager.cacheNameBytes(cacheName), clientTopology, flags, cfg, listenerId, dataFormat, listener, cacheName,
            listenerNotifier, telemetryService);
      this.filterFactoryParams = filterFactoryParams;
      this.converterFactoryParams = converterFactoryParams;
      this.remoteCache = remoteCache;
      this.telemetryService = telemetryService;
   }

   public AddClientListenerOperation copy() {
      return new AddClientListenerOperation(codec, channelFactory, cacheNameString, header.getClientTopology(), flags(), cfg,
            listenerId, listenerNotifier, listener, filterFactoryParams, converterFactoryParams, dataFormat(),
            remoteCache, telemetryService);
   }

   @Override
   protected void actualExecute(Channel channel) {
      ClientListener clientListener = extractClientListener();

      channel.pipeline().get(HeaderDecoder.class).registerOperation(channel, this);

      listenerNotifier.addDispatcher(ClientEventDispatcher.create(this,
            address, () -> cleanup(channel), remoteCache));

      ByteBuf buf = channel.alloc().buffer();

      codec.writeHeader(buf, header);
      ByteBufUtil.writeArray(buf, listenerId);
      codec.writeClientListenerParams(buf, clientListener, filterFactoryParams, converterFactoryParams);
      codec.writeClientListenerInterests(buf, ClientEventDispatcher.findMethods(listener).keySet());
      channel.writeAndFlush(buf);
   }
}
