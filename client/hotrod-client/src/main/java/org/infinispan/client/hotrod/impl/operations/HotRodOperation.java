package org.infinispan.client.hotrod.impl.operations;

import java.net.SocketAddress;
import java.net.SocketTimeoutException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HeaderParams;
import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.client.hotrod.impl.transport.netty.ByteBufUtil;
import org.infinispan.client.hotrod.impl.transport.netty.HeaderDecoder;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelFactory;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.EventLoop;
import io.netty.handler.codec.DecoderException;
import net.jcip.annotations.Immutable;

/**
 * Generic Hot Rod operation. It is aware of {@link org.infinispan.client.hotrod.Flag}s and it is targeted against a
 * cache name. This base class encapsulates the knowledge of writing and reading a header, as described in the
 * <a href="http://community.jboss.org/wiki/HotRodProtocol">Hot Rod protocol specification</a>
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Immutable
public abstract class HotRodOperation<T> extends CompletableFuture<T> implements
      HotRodConstants, Runnable {
   private static final Log log = LogFactory.getLog(HotRodOperation.class);
   private static final boolean trace = log.isTraceEnabled();

   private static final AtomicLong MSG_ID = new AtomicLong(1);

   public final byte[] cacheName;
   protected final int flags;
   protected final Codec codec;
   protected final Configuration cfg;
   protected final ChannelFactory channelFactory;
   protected final HeaderParams header;
   protected volatile ScheduledFuture<?> timeoutFuture;

   private static final byte NO_TX = 0;
   private static final byte XA_TX = 1;

   protected HotRodOperation(short requestCode, short responseCode, Codec codec, int flags, Configuration cfg, byte[] cacheName, AtomicInteger topologyId, ChannelFactory channelFactory) {
      this.flags = flags;
      this.cfg = cfg;
      this.cacheName = cacheName;
      this.codec = codec;
      this.channelFactory = channelFactory;
      // TODO: we could inline all the header here
      this.header = new HeaderParams(requestCode, responseCode, MSG_ID.getAndIncrement())
            .cacheName(cacheName).flags(flags)
            .clientIntel(cfg.clientIntelligence())
            .topologyId(topologyId).txMarker(NO_TX)
            .topologyAge(channelFactory.getTopologyAge());
   }

   public abstract CompletableFuture<T> execute();

   public HeaderParams header() {
      return header;
   }

   protected void sendHeaderAndRead(Channel channel) {
      scheduleRead(channel);
      sendHeader(channel);
   }

   protected void sendHeader(Channel channel) {
      ByteBuf buf = channel.alloc().buffer(codec.estimateHeaderSize(header));
      codec.writeHeader(buf, header);
      channel.writeAndFlush(buf);
   }

   protected void scheduleRead(Channel channel) {
      channel.pipeline().get(HeaderDecoder.class).registerOperation(channel, this);
   }

   public void releaseChannel(Channel channel) {
      channelFactory.releaseChannel(channel);
   }

   public void channelInactive(Channel channel) {
      SocketAddress address = channel.remoteAddress();
      completeExceptionally(log.connectionClosed(address, address));
   }

   public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
      while (cause instanceof DecoderException && cause.getCause() != null) {
         cause = cause.getCause();
      }
      try {
         if (cause instanceof HotRodClientException && ((HotRodClientException) cause).isServerError()) {
            // don't close the channel, server just sent an error, there's nothing wrong with the channel
         } else {
            if (trace) {
               log.tracef(cause, "Requesting %s close due to exception", ctx.channel());
            }
            ctx.close();
         }
      } finally {
         completeExceptionally(cause);
      }
   }

   protected void sendArrayOperation(Channel channel, byte[] array) {
      // 1) write [header][array length][key]
      ByteBuf buf = channel.alloc().buffer(codec.estimateHeaderSize(header) + ByteBufUtil.estimateArraySize(array));

      codec.writeHeader(buf, header);
      ByteBufUtil.writeArray(buf, array);
      channel.writeAndFlush(buf);
   }

   public abstract void acceptResponse(ByteBuf buf, short status, HeaderDecoder decoder);

   @Override
   public String toString() {
      String cn = cacheName == null || cacheName.length == 0 ? "(default)" : new String(cacheName);
      StringBuilder sb = new StringBuilder(64);
      sb.append(getClass().getSimpleName()).append('{').append(cn);
      addParams(sb);
      sb.append(", flags=").append(Integer.toHexString(flags)).append('}');
      return sb.toString();
   }

   protected void addParams(StringBuilder sb) {
   }

   @Override
   public boolean complete(T value) {
      // Timeout future is not set if the operation completes before scheduling a read:
      // see RemoveClientListenerOperation.fetchChannelAndInvoke
      if (timeoutFuture != null) {
         timeoutFuture.cancel(false);
      }
      return super.complete(value);
   }

   public void scheduleTimeout(EventLoop eventLoop) {
      this.timeoutFuture = eventLoop.schedule(this, channelFactory.socketTimeout(), TimeUnit.MILLISECONDS);
   }

   @Override
   public void run() {
      completeExceptionally(new SocketTimeoutException(this + " timed out after " + channelFactory.socketTimeout() + " ms"));
   }
}
