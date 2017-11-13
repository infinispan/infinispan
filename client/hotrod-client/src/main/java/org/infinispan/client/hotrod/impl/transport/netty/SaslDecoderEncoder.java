package org.infinispan.client.hotrod.impl.transport.netty;


import javax.security.sasl.SaslClient;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;

public class SaslDecoderEncoder implements ChannelInboundHandlerDefaults, ChannelOutboundHandlerDefaults {
   private final SaslClient saslClient;

   public SaslDecoderEncoder(SaslClient saslClient) {
      this.saslClient = saslClient;
   }

   @Override
   public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
      if (!(msg instanceof ByteBuf)) {
         throw new IllegalArgumentException(String.valueOf(msg));
      }
      ByteBuf buf = (ByteBuf) msg;
      // the correct buf size is guaranteed by prepended LengthFieldBaseFrameDecoder
      byte[] decoded;
      if (buf.hasArray()) {
         decoded = saslClient.unwrap(buf.array(), buf.arrayOffset() + buf.readerIndex(), buf.readableBytes());
      } else {
         byte[] bytes = new byte[buf.readableBytes()];
         buf.getBytes(buf.readerIndex(), bytes);
         decoded = saslClient.unwrap(bytes, 0, bytes.length);
      }
      buf.release();
      ctx.fireChannelRead(Unpooled.wrappedBuffer(decoded));
   }

   @Override
   public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
      if (!(msg instanceof ByteBuf)) {
         throw new IllegalArgumentException(String.valueOf(msg));
      }
      ByteBuf buf = (ByteBuf) msg;
      byte[] encoded;
      if (buf.hasArray()) {
         encoded = saslClient.wrap(buf.array(), buf.arrayOffset() + buf.readerIndex(), buf.readableBytes());
      } else {
         byte[] bytes = new byte[buf.readableBytes()];
         buf.getBytes(buf.readerIndex(), bytes);
         encoded = saslClient.wrap(bytes, 0, bytes.length);
      }
      buf.release();
      ctx.write(Unpooled.wrappedBuffer(Unpooled.copyInt(encoded.length), Unpooled.wrappedBuffer(encoded)), promise);
   }

   @Override
   public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
      ctx.fireExceptionCaught(cause);
   }

   @Override
   public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
      // noop
   }

   @Override
   public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
      // noop
   }
}
