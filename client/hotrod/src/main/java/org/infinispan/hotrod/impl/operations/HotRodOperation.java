package org.infinispan.hotrod.impl.operations;

import static org.infinispan.hotrod.impl.logging.Log.HOTROD;

import java.net.SocketAddress;
import java.net.SocketTimeoutException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.infinispan.api.common.CacheEntryExpiration;
import org.infinispan.api.common.CacheOptions;
import org.infinispan.api.common.CacheWriteOptions;
import org.infinispan.hotrod.HotRodFlag;
import org.infinispan.hotrod.HotRodFlags;
import org.infinispan.hotrod.exceptions.HotRodClientException;
import org.infinispan.hotrod.impl.DataFormat;
import org.infinispan.hotrod.impl.logging.Log;
import org.infinispan.hotrod.impl.logging.LogFactory;
import org.infinispan.hotrod.impl.protocol.HeaderParams;
import org.infinispan.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.hotrod.impl.transport.netty.ByteBufUtil;
import org.infinispan.hotrod.impl.transport.netty.HeaderDecoder;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.handler.codec.DecoderException;

/**
 * Generic Hot Rod operation. It is aware of {@link HotRodFlag}s and it is targeted against a cache name. This base
 * class encapsulates the knowledge of writing and reading a header, as described in the
 * <a href="http://community.jboss.org/wiki/HotRodProtocol">Hot Rod protocol specification</a>
 *
 * @since 14.0
 */
public abstract class HotRodOperation<T> extends CompletableFuture<T> implements HotRodConstants, Runnable {
   private static final Log log = LogFactory.getLog(HotRodOperation.class);
   private static final AtomicLong MSG_ID = new AtomicLong(1);
   private static final byte NO_TX = 0;
   protected final OperationContext operationContext;
   protected final CacheOptions.Impl options;
   protected final HeaderParams header;
   protected volatile ScheduledFuture<?> timeoutFuture;
   private Channel channel;

   protected HotRodOperation(OperationContext operationContext, short requestCode, short responseCode, CacheOptions options, DataFormat dataFormat) {
      this.operationContext = operationContext;
      this.options = (CacheOptions.Impl) options;
      // TODO: we could inline all the header here
      this.header = new HeaderParams(requestCode, responseCode, flags(), NO_TX, MSG_ID.getAndIncrement(), dataFormat, operationContext.getClientTopology())
            .cacheName(operationContext.getCacheNameBytes())
            .topologyAge(operationContext.getChannelFactory().getTopologyAge());
   }

   protected HotRodOperation(OperationContext operationContext, short requestCode, short responseCode, CacheOptions options) {
      this(operationContext, requestCode, responseCode, options, null);
   }

   public abstract CompletionStage<T> execute();

   public HeaderParams header() {
      return header;
   }

   protected int flags() {
      int flags = HotRodFlags.toInt(options);
      if (options instanceof CacheWriteOptions) {
         CacheEntryExpiration.Impl expiration = (CacheEntryExpiration.Impl) ((CacheWriteOptions) options).expiration();
         if (expiration.rawLifespan() == null) {
            flags |= HotRodFlag.DEFAULT_LIFESPAN.getFlagInt();
         }
         if (expiration.rawMaxIdle() == null) {
            flags |= HotRodFlag.DEFAULT_MAXIDLE.getFlagInt();
         }
      }
      return flags;
   }

   protected void sendHeaderAndRead(Channel channel) {
      scheduleRead(channel);
      sendHeader(channel);
   }

   protected void sendHeader(Channel channel) {
      ByteBuf buf = channel.alloc().buffer(operationContext.getCodec().estimateHeaderSize(header));
      operationContext.getCodec().writeHeader(buf, header);
      channel.writeAndFlush(buf);
   }

   protected void scheduleRead(Channel channel) {
      channel.pipeline().get(HeaderDecoder.class).registerOperation(channel, this);
   }

   public void releaseChannel(Channel channel) {
      operationContext.getChannelFactory().releaseChannel(channel);
   }

   public void channelInactive(Channel channel) {
      SocketAddress address = channel.remoteAddress();
      completeExceptionally(log.connectionClosed(address, address));
   }

   public void exceptionCaught(Channel channel, Throwable cause) {
      while (cause instanceof DecoderException && cause.getCause() != null) {
         cause = cause.getCause();
      }
      try {
         if (cause instanceof HotRodClientException && ((HotRodClientException) cause).isServerError()) {
            // don't close the channel, server just sent an error, there's nothing wrong with the channel
         } else {
            HOTROD.closingChannelAfterError(channel, cause);
            channel.close();
         }
      } finally {
         completeExceptionally(cause);
      }
   }

   protected void sendArrayOperation(Channel channel, byte[] array) {
      // 1) write [header][array length][key]
      ByteBuf buf = channel.alloc().buffer(operationContext.getCodec().estimateHeaderSize(header) + ByteBufUtil.estimateArraySize(array));
      operationContext.getCodec().writeHeader(buf, header);
      ByteBufUtil.writeArray(buf, array);
      channel.writeAndFlush(buf);
   }

   public abstract void acceptResponse(ByteBuf buf, short status, HeaderDecoder decoder);

   @Override
   public String toString() {
      byte[] cacheName = operationContext.getCacheNameBytes();
      String cn = cacheName == null || cacheName.length == 0 ? "(default)" : new String(cacheName);
      StringBuilder sb = new StringBuilder(64);
      sb.append(getClass().getSimpleName()).append('{').append(cn);
      addParams(sb);
      sb.append(", flags=").append(Integer.toHexString(flags()));
      if (channel != null) {
         sb.append(", connection=").append(channel.remoteAddress());
      }
      sb.append('}');

      return sb.toString();
   }

   protected void addParams(StringBuilder sb) {
   }

   @Override
   public boolean complete(T value) {
      cancelTimeout();
      return super.complete(value);
   }

   @Override
   public boolean completeExceptionally(Throwable ex) {
      cancelTimeout();
      return super.completeExceptionally(ex);
   }

   public void scheduleTimeout(Channel channel) {
      assert timeoutFuture == null;
      this.channel = channel;
      this.timeoutFuture = channel.eventLoop().schedule(this, operationContext.getChannelFactory().socketTimeout(), TimeUnit.MILLISECONDS);
   }

   private void cancelTimeout() {
      // Timeout future is not set if the operation completes before scheduling a read:
      // see RemoveClientListenerOperation.fetchChannelAndInvoke
      if (timeoutFuture != null) {
         timeoutFuture.cancel(false);
      }
   }

   @Override
   public void run() {
      exceptionCaught(channel, new SocketTimeoutException(this + " timed out after " + operationContext.getChannelFactory().socketTimeout() + " ms"));
   }

   public final DataFormat dataFormat() {
      return header.dataFormat();
   }

   protected final byte[] cacheName() {
      return header.cacheName();
   }
}
