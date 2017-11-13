package org.infinispan.client.hotrod.impl.operations;

import java.net.SocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HeaderParams;
import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.client.hotrod.impl.transport.netty.ByteBufUtil;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelInboundHandlerDefaults;
import org.infinispan.client.hotrod.impl.transport.netty.HeaderDecoder;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelFactory;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
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
      HotRodConstants, ChannelInboundHandlerDefaults {
   private static final Log log = LogFactory.getLog(HotRodOperation.class);

   public final byte[] cacheName;
   protected final int flags;
   protected final AtomicInteger topologyId;
   protected final Codec codec;
   protected final Configuration cfg;
   protected final ChannelFactory channelFactory;

   private static final byte NO_TX = 0;
   private static final byte XA_TX = 1;

   protected HotRodOperation(Codec codec, int flags, Configuration cfg, byte[] cacheName, AtomicInteger topologyId, ChannelFactory channelFactory) {
      this.flags = flags;
      this.cfg = cfg;
      this.cacheName = cacheName;
      this.topologyId = topologyId;
      this.codec = codec;
      this.channelFactory = channelFactory;
   }

   public abstract CompletableFuture<T> execute();

   protected final HeaderParams headerParams(short operationCode) {
      return createHeader()
            .opCode(operationCode).cacheName(cacheName).flags(flags)
            .clientIntel(cfg.clientIntelligence())
            .topologyId(topologyId).txMarker(NO_TX)
            .topologyAge(channelFactory.getTopologyAge());
   }

   protected void sendHeaderAndRead(Channel channel, byte operationCode) {
      HeaderParams header = headerParams(operationCode);
      scheduleRead(channel, header);
      sendHeader(channel, header);
   }

   protected HeaderParams createHeader() {
      return new HeaderParams();
   }

   protected void sendHeader(Channel channel, HeaderParams header) {
      ByteBuf buf = channel.alloc().buffer(codec.estimateHeaderSize(header));
      codec.writeHeader(buf, header);
      channel.writeAndFlush(buf);
   }

   protected HeaderDecoder<T> scheduleRead(Channel channel, HeaderParams header) {
      HeaderDecoder<T> decoder = new HeaderDecoder<>(codec, header, channelFactory, this);
      channel.pipeline().addLast(decoder, this);
      return decoder;
   }

   public void releaseChannel(Channel channel) {
      channelFactory.releaseChannel(channel);
   }

   @Override
   public void channelInactive(ChannelHandlerContext ctx) throws Exception {
      SocketAddress address = ctx.channel().remoteAddress();
      completeExceptionally(log.connectionClosed(address, address));
   }

   @Override
   public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
      try {
         ctx.pipeline().remove(this);
         ctx.close();
      } finally {
         while (cause instanceof DecoderException && cause.getCause() != null) {
            cause = cause.getCause();
         }
         completeExceptionally(cause);
      }
   }

   protected void sendArrayOperation(Channel channel, HeaderParams header, byte[] array) {
      // 1) write [header][array length][key]
      ByteBuf buf = channel.alloc().buffer(codec.estimateHeaderSize(header) + ByteBufUtil.estimateArraySize(array));

      codec.writeHeader(buf, header);
      ByteBufUtil.writeArray(buf, array);
      channel.writeAndFlush(buf);
   }

   public abstract T decodePayload(ByteBuf buf, short status);

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
}
