package org.infinispan.client.hotrod.impl.operations;

import java.util.concurrent.atomic.AtomicReference;

import org.infinispan.client.hotrod.DataFormat;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.event.impl.ClientEventDispatcher;
import org.infinispan.client.hotrod.event.impl.ClientListenerNotifier;
import org.infinispan.client.hotrod.impl.ClientTopology;
import org.infinispan.client.hotrod.impl.InternalRemoteCache;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.transport.netty.ByteBufUtil;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelFactory;
import org.infinispan.client.hotrod.impl.transport.netty.HeaderDecoder;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

/**
 * @author Galder Zamarreño
 */
public class AddBloomNearCacheClientListenerOperation extends ClientListenerOperation {

   private final int bloomFilterBits;
   private final InternalRemoteCache<?, ?> remoteCache;

   protected AddBloomNearCacheClientListenerOperation(Codec codec, ChannelFactory channelFactory,
                                                      String cacheName, AtomicReference<ClientTopology> clientTopology, int flags, Configuration cfg,
                                                      ClientListenerNotifier listenerNotifier, Object listener,
                                                      DataFormat dataFormat,
                                                      int bloomFilterBits, InternalRemoteCache<?, ?> remoteCache) {
      this(codec, channelFactory, cacheName, clientTopology, flags, cfg, generateListenerId(),
            listenerNotifier, listener, dataFormat, bloomFilterBits,
            remoteCache);
   }

   private AddBloomNearCacheClientListenerOperation(Codec codec, ChannelFactory channelFactory,
                                                    String cacheName, AtomicReference<ClientTopology> clientTopology, int flags, Configuration cfg,
                                                    byte[] listenerId, ClientListenerNotifier listenerNotifier, Object listener,
                                                    DataFormat dataFormat,
                                                    int bloomFilterBits, InternalRemoteCache<?, ?> remoteCache) {
      super(ADD_BLOOM_FILTER_NEAR_CACHE_LISTENER_REQUEST, ADD_BLOOM_FILTER_NEAR_CACHE_LISTENER_RESPONSE, codec, channelFactory,
            RemoteCacheManager.cacheNameBytes(cacheName), clientTopology, flags, cfg, listenerId, dataFormat, listener,
            cacheName, listenerNotifier, null);
      this.bloomFilterBits = bloomFilterBits;
      this.remoteCache = remoteCache;
   }

   public AddBloomNearCacheClientListenerOperation copy() {
      return new AddBloomNearCacheClientListenerOperation(codec, channelFactory, cacheNameString, header.getClientTopology(), flags(), cfg,
            listenerId, listenerNotifier, listener, dataFormat(),
            bloomFilterBits, remoteCache);
   }

   @Override
   protected void actualExecute(Channel channel) {
      channel.pipeline().get(HeaderDecoder.class).registerOperation(channel, this);

      listenerNotifier.addDispatcher(ClientEventDispatcher.create(this,
            address, () -> cleanup(channel), remoteCache));

      ByteBuf buf = channel.alloc().buffer();

      codec.writeHeader(buf, header);
      ByteBufUtil.writeArray(buf, listenerId);
      codec.writeBloomFilter(buf, bloomFilterBits);
      channel.writeAndFlush(buf);
   }
}
