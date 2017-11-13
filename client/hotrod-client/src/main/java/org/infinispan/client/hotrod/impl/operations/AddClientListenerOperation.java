package org.infinispan.client.hotrod.impl.operations;

import java.nio.ByteBuffer;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.annotation.ClientListener;
import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.event.ClientEvent;
import org.infinispan.client.hotrod.event.impl.ClientEventDispatcher;
import org.infinispan.client.hotrod.event.impl.ClientListenerNotifier;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HeaderParams;
import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.client.hotrod.impl.transport.netty.ByteBufUtil;
import org.infinispan.client.hotrod.impl.transport.netty.HeaderOrEventDecoder;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelFactory;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;
import org.infinispan.commons.util.ReflectionUtil;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

/**
 * @author Galder Zamarreño
 */
public class AddClientListenerOperation extends RetryOnFailureOperation<Short> implements Consumer<ClientEvent> {

   private static final Log log = LogFactory.getLog(AddClientListenerOperation.class, Log.class);

   public final byte[] listenerId;
   private final String cacheNameString;

   /**
    * Decicated transport instance for adding client listener. This transport
    * is used to send events back to client and it's only released when the
    * client listener is removed.
    */
   private Channel dedicatedChannel;

   private final ClientListenerNotifier listenerNotifier;
   public final Object listener;
   public final byte[][] filterFactoryParams;
   public final byte[][] converterFactoryParams;

   protected AddClientListenerOperation(Codec codec, ChannelFactory channelFactory,
                                        String cacheName, AtomicInteger topologyId, int flags, Configuration cfg,
                                        ClientListenerNotifier listenerNotifier, Object listener,
                                        byte[][] filterFactoryParams, byte[][] converterFactoryParams) {
      this(codec, channelFactory, cacheName, topologyId, flags, cfg, generateListenerId(),
            listenerNotifier, listener, filterFactoryParams, converterFactoryParams);
   }

   protected AddClientListenerOperation(Codec codec, ChannelFactory channelFactory,
                                        String cacheName, AtomicInteger topologyId, int flags, Configuration cfg,
                                        byte[] listenerId, ClientListenerNotifier listenerNotifier, Object listener,
                                        byte[][] filterFactoryParams, byte[][] converterFactoryParams) {
      super(codec, channelFactory, RemoteCacheManager.cacheNameBytes(cacheName), topologyId, flags, cfg);
      this.listenerId = listenerId;
      this.listenerNotifier = listenerNotifier;
      this.listener = listener;
      this.filterFactoryParams = filterFactoryParams;
      this.converterFactoryParams = converterFactoryParams;
      this.cacheNameString = cacheName;
   }

   public AddClientListenerOperation copy() {
      return new AddClientListenerOperation(codec, channelFactory, cacheNameString, topologyId, flags, cfg,
            listenerId, listenerNotifier, listener, filterFactoryParams, converterFactoryParams);
   }

   private static byte[] generateListenerId() {
      ThreadLocalRandom random = ThreadLocalRandom.current();
      byte[] listenerId = new byte[16];
      ByteBuffer bb = ByteBuffer.wrap(listenerId);
      bb.putLong(random.nextLong());
      bb.putLong(random.nextLong());
      return listenerId;
   }

   private ClientListener extractClientListener() {
      ClientListener l = ReflectionUtil.getAnnotation(listener.getClass(), ClientListener.class);
      if (l == null)
         throw log.missingClientListenerAnnotation(listener.getClass().getName());
      return l;
   }

   public String getCacheName() {
      return cacheNameString;
   }

   public Channel getDedicatedChannel() {
      return dedicatedChannel;
   }

   @Override
   protected void executeOperation(Channel channel) {
      ClientListener clientListener = extractClientListener();

      HeaderParams header = headerParams(ADD_CLIENT_LISTENER_REQUEST);
      channel.pipeline().addLast(new HeaderOrEventDecoder(codec, header, channelFactory, this, this, listenerId, cfg), this);

      dedicatedChannel = channel;
      listenerNotifier.addDispatcher(ClientEventDispatcher.create(this, listenerNotifier));

      ByteBuf buf = channel.alloc().buffer();

      codec.writeHeader(buf, header);
      ByteBufUtil.writeArray(buf, listenerId);
      codec.writeClientListenerParams(buf, clientListener, filterFactoryParams, converterFactoryParams);
      codec.writeClientListenerInterests(buf, ClientEventDispatcher.findMethods(listener).keySet());
      channel.writeAndFlush(buf);
   }

   @Override
   public void releaseChannel(Channel channel) {
      // Do not release the channel
   }

   @Override
   public Short decodePayload(ByteBuf buf, short status) {
      if (HotRodConstants.isSuccess(status)) {
         listenerNotifier.startClientListener(listenerId);
      } else {
         // this releases the channel
         listenerNotifier.removeClientListener(listenerId);
         throw log.failedToAddListener(listener, status);
      }
      return status;
   }

   @Override
   public void accept(ClientEvent clientEvent) {
      listenerNotifier.invokeEvent(listenerId, clientEvent);
   }
}
