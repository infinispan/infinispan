package org.infinispan.client.hotrod.impl.operations;

import java.nio.ByteBuffer;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.client.hotrod.DataFormat;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.annotation.ClientListener;
import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.event.impl.ClientEventDispatcher;
import org.infinispan.client.hotrod.event.impl.ClientListenerNotifier;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.client.hotrod.impl.transport.netty.ByteBufUtil;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelFactory;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelRecord;
import org.infinispan.client.hotrod.impl.transport.netty.HeaderDecoder;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;
import org.infinispan.commons.util.ReflectionUtil;
import org.infinispan.commons.util.Util;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

/**
 * @author Galder Zamarreño
 */
public class AddClientListenerOperation extends RetryOnFailureOperation<Short> {

   private static final Log log = LogFactory.getLog(AddClientListenerOperation.class, Log.class);

   public final byte[] listenerId;
   public final Object listener;
   private final String cacheNameString;
   private final ClientListenerNotifier listenerNotifier;
   private final byte[][] filterFactoryParams;
   private final byte[][] converterFactoryParams;

   protected AddClientListenerOperation(Codec codec, ChannelFactory channelFactory,
                                        String cacheName, AtomicInteger topologyId, int flags, Configuration cfg,
                                        ClientListenerNotifier listenerNotifier, Object listener,
                                        byte[][] filterFactoryParams, byte[][] converterFactoryParams, DataFormat dataFormat) {
      this(codec, channelFactory, cacheName, topologyId, flags, cfg, generateListenerId(),
            listenerNotifier, listener, filterFactoryParams, converterFactoryParams, dataFormat);
   }

   private AddClientListenerOperation(Codec codec, ChannelFactory channelFactory,
                                      String cacheName, AtomicInteger topologyId, int flags, Configuration cfg,
                                      byte[] listenerId, ClientListenerNotifier listenerNotifier, Object listener,
                                      byte[][] filterFactoryParams, byte[][] converterFactoryParams, DataFormat dataFormat) {
      super(ADD_CLIENT_LISTENER_REQUEST, ADD_CLIENT_LISTENER_RESPONSE, codec, channelFactory, RemoteCacheManager.cacheNameBytes(cacheName), topologyId, flags, cfg, dataFormat);
      this.listenerId = listenerId;
      this.listenerNotifier = listenerNotifier;
      this.listener = listener;
      this.filterFactoryParams = filterFactoryParams;
      this.converterFactoryParams = converterFactoryParams;
      this.cacheNameString = cacheName;
   }

   public AddClientListenerOperation copy() {
      return new AddClientListenerOperation(codec, channelFactory, cacheNameString, header.topologyId(), flags, cfg,
            listenerId, listenerNotifier, listener, filterFactoryParams, converterFactoryParams, dataFormat);
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

   @Override
   protected void executeOperation(Channel channel) {
      // Note: since the HeaderDecoder now supports decoding both operations and events we don't have to
      // wait until all operations complete; the server will deliver responses and we'll just handle them regardless
      // of the order
      if (!channel.isActive()) {
         channelInactive(channel);
         return;
      }
      ClientListener clientListener = extractClientListener();

      boolean usesRawData = clientListener.useRawData();
      boolean usesFilter = !(clientListener.converterFactoryName().equals("") && clientListener.filterFactoryName().equals(""));
      boolean customDataFormat = dataFormat != null && dataFormat.hasCustomFormat();

      if (customDataFormat && usesFilter && !usesRawData) {
         throw log.clientListenerMustUseRawDataWithCustomDataFormat();
      }

      channel.pipeline().get(HeaderDecoder.class).registerOperation(channel, this);

      listenerNotifier.addDispatcher(ClientEventDispatcher.create(this,
            ChannelRecord.of(channel).getUnresolvedAddress(),
            () -> cleanup(channel)));

      ByteBuf buf = channel.alloc().buffer();

      codec.writeHeader(buf, header);
      ByteBufUtil.writeArray(buf, listenerId);
      codec.writeClientListenerParams(buf, clientListener, filterFactoryParams, converterFactoryParams);
      codec.writeClientListenerInterests(buf, ClientEventDispatcher.findMethods(listener).keySet());
      channel.writeAndFlush(buf);
   }

   private void cleanup(Channel channel) {
      channel.eventLoop().execute(() -> {
         if (!codec.allowOperationsAndEvents()) {
            if (channel.isOpen()) {
               channelFactory.releaseChannel(channel);
            }
         }
         HeaderDecoder decoder = channel.pipeline().get(HeaderDecoder.class);
         if (decoder != null) {
            decoder.removeListener(listenerId);
         }
      });
   }

   @Override
   public void releaseChannel(Channel channel) {
      if (codec.allowOperationsAndEvents()) {
         super.releaseChannel(channel);
      }
   }

   @Override
   public void acceptResponse(ByteBuf buf, short status, HeaderDecoder decoder) {
      if (HotRodConstants.isSuccess(status)) {
         decoder.addListener(listenerId);
         listenerNotifier.startClientListener(listenerId);
      } else {
         // this releases the channel
         listenerNotifier.removeClientListener(listenerId);
         throw log.failedToAddListener(listener, status);
      }
      complete(status);
   }

   @Override
   public boolean completeExceptionally(Throwable ex) {
      if (!isDone()) {
         listenerNotifier.removeClientListener(listenerId);
      }
      return super.completeExceptionally(ex);
   }

   public void postponeTimeout(Channel channel) {
      assert !isDone();
      timeoutFuture.cancel(false);
      timeoutFuture = null;
      scheduleTimeout(channel.eventLoop());
   }

   @Override
   protected void addParams(StringBuilder sb) {
      sb.append("listenerId=").append(Util.printArray(listenerId));
   }

   public DataFormat getDataFormat() {
      return dataFormat;
   }
}
