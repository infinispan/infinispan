package org.infinispan.client.hotrod.impl.operations;

import static org.infinispan.client.hotrod.logging.Log.HOTROD;

import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicReference;

import org.infinispan.client.hotrod.DataFormat;
import org.infinispan.client.hotrod.annotation.ClientListener;
import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.event.impl.ClientListenerNotifier;
import org.infinispan.client.hotrod.impl.ClientTopology;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelFactory;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelRecord;
import org.infinispan.client.hotrod.impl.transport.netty.HeaderDecoder;
import org.infinispan.client.hotrod.telemetry.impl.TelemetryService;
import org.infinispan.commons.util.ReflectionUtil;
import org.infinispan.commons.util.Util;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

public abstract class ClientListenerOperation extends RetryOnFailureOperation<SocketAddress> {
   public final byte[] listenerId;
   public final Object listener;
   protected final String cacheNameString;
   protected final ClientListenerNotifier listenerNotifier;

   // Holds which address we are currently executing the operation on
   protected SocketAddress address;

   protected ClientListenerOperation(short requestCode, short responseCode, Codec codec, ChannelFactory channelFactory,
                                     byte[] cacheName, AtomicReference<ClientTopology> clientTopology, int flags, Configuration cfg,
                                     byte[] listenerId, DataFormat dataFormat, Object listener, String cacheNameString,
                                     ClientListenerNotifier listenerNotifier, TelemetryService telemetryService) {
      super(requestCode, responseCode, codec, channelFactory, cacheName, clientTopology, flags, cfg, dataFormat, telemetryService);
      this.listenerId = listenerId;
      this.listener = listener;
      this.cacheNameString = cacheNameString;
      this.listenerNotifier = listenerNotifier;
   }

   protected static byte[] generateListenerId() {
      ThreadLocalRandom random = ThreadLocalRandom.current();
      byte[] listenerId = new byte[16];
      ByteBuffer bb = ByteBuffer.wrap(listenerId);
      bb.putLong(random.nextLong());
      bb.putLong(random.nextLong());
      return listenerId;
   }

   protected ClientListener extractClientListener() {
      ClientListener l = ReflectionUtil.getAnnotation(listener.getClass(), ClientListener.class);
      if (l == null)
         throw HOTROD.missingClientListenerAnnotation(listener.getClass().getName());
      return l;
   }

   public String getCacheName() {
      return cacheNameString;
   }

   @Override
   protected final void executeOperation(Channel channel) {
      // Note: since the HeaderDecoder now supports decoding both operations and events we don't have to
      // wait until all operations complete; the server will deliver responses and we'll just handle them regardless
      // of the order
      if (!channel.isActive()) {
         channelInactive(channel);
         return;
      }
      this.address = ChannelRecord.of(channel).getUnresolvedAddress();
      actualExecute(channel);
   }

   protected abstract void actualExecute(Channel channel);

   protected void cleanup(Channel channel) {
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
         throw HOTROD.failedToAddListener(listener, status);
      }
      complete(address);
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
      scheduleTimeout(channel);
   }

   @Override
   protected void addParams(StringBuilder sb) {
      sb.append("listenerId=").append(Util.printArray(listenerId));
   }


   public abstract ClientListenerOperation copy();
}
