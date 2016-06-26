package org.infinispan.server.core.transport;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandler;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.TooLongFrameException;

import java.net.SocketAddress;
import java.util.List;

import javax.security.sasl.Sasl;
import javax.security.sasl.SaslServer;

/**
  * Handles QOP of the SASL protocol.
  */
public class SaslQopHandler extends ByteToMessageDecoder implements ChannelOutboundHandler {
     private final SaslServer server;
     private final int maxBufferSize;
     private final int maxSendBufferSize;
     private int packetLength = -1;

     public SaslQopHandler(SaslServer server) {
         this.server = server;
         String maxBuf = (String) server.getNegotiatedProperty(Sasl.MAX_BUFFER);
         if (maxBuf != null) {
             maxBufferSize = Integer.parseInt(maxBuf);
         } else {
             maxBufferSize = -1;
         }
         String maxSendBuf = (String) server.getNegotiatedProperty(Sasl.RAW_SEND_SIZE);
         if (maxSendBuf != null) {
             maxSendBufferSize = Integer.parseInt(maxSendBuf);
         } else {
             maxSendBufferSize = -1;
         }
     }

     private static byte[] readBytes(ByteBuf buffer) {
        byte[] bytes = new byte[buffer.readableBytes()];
        buffer.readBytes(bytes);
        buffer.release();
        return bytes;
    }

     @Override
     public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
         ByteBuf buffer = (ByteBuf) msg;
         byte[] bytes;
         int offset;
         int len;
         if (buffer.hasArray()) {
             bytes = buffer.array();
             offset = buffer.arrayOffset() + buffer.readerIndex();
             len = buffer.readableBytes();
         } else {
             bytes = readBytes(buffer);
             offset = 0;
             len = bytes.length;
         }
         byte[] wrapped = server.wrap(bytes, offset, len);
         ctx.write(ctx.alloc().buffer(4).writeInt(wrapped.length));
         if (maxSendBufferSize != -1 && wrapped.length > maxSendBufferSize) {
             // The produces data is bigger then the maxSendBufferSize so split it and flush every of them directly.
             int size = wrapped.length;
             int off = 0;
             for (;;) {
                 if (size < maxSendBufferSize) {
                     ctx.writeAndFlush(Unpooled.wrappedBuffer(wrapped, off, size), promise);
                     return;
                 } else {
                     ctx.writeAndFlush(Unpooled.wrappedBuffer(wrapped, off, maxSendBufferSize));
                     off += maxSendBufferSize;
                     size -= maxSendBufferSize;
                 }
             }
         } else {
             ctx.write(Unpooled.wrappedBuffer(wrapped), promise);
         }
     }

     @Override
     protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
         int len = packetLength;

         if (len == -1) {
             if (in.readableBytes() < 4) {
                 return;
             }
             len = packetLength = (int) in.readUnsignedInt();
             if (maxBufferSize != -1 && maxBufferSize < packetLength) {
                 TooLongFrameException ex = new TooLongFrameException(
                         "Frame exceed exceed max buffer size: " + packetLength + " > " + maxBufferSize);
                 ctx.fireExceptionCaught(ex);
                 ctx.close();
                 return;
             }
         }
         if (len > in.readableBytes()) {
             return;
         }
         // reset packet length
         packetLength = -1;
         int offset;
         byte[] array;
         if  (in.hasArray()) {
             offset = in.readerIndex() + in.arrayOffset();
             array = in.array();
             in.skipBytes(len);
         } else {
             offset = 0;
             array = new byte[len];
             in.readBytes(array);
         }
         out.add(Unpooled.wrappedBuffer(server.unwrap(array, offset, len)));
     }

     @Override
     public void bind(ChannelHandlerContext ctx, SocketAddress localAddress, ChannelPromise promise)
             throws Exception {
         ctx.bind(localAddress, promise);
     }

     @Override
     public void connect(ChannelHandlerContext ctx, SocketAddress remoteAddress,
                         SocketAddress localAddress, ChannelPromise promise) throws Exception {
         ctx.connect(remoteAddress, localAddress, promise);
     }

     @Override
     public void disconnect(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
         ctx.disconnect(promise);
     }

     @Override
     public void close(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
         ctx.close(promise);
     }

     @Override
     public void deregister(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
         ctx.deregister(promise);
     }

     @Override
     public void read(ChannelHandlerContext ctx) throws Exception {
         ctx.read();
     }

     @Override
     public void flush(ChannelHandlerContext ctx) throws Exception {
         ctx.flush();
     }

     @Override
     protected void handlerRemoved0(ChannelHandlerContext ctx) throws Exception {
         super.handlerRemoved0(ctx);
         server.dispose();
     }
 }