package org.infinispan.server.core.transport

import java.net.SocketAddress

import io.netty.buffer.ByteBuf
import io.netty.channel._

/**
 * Input/Output ChannelHandler to keep statistics
 *
 * @author gustavonalle
 * @since 7.1
 */
trait StatsChannelHandler extends ChannelInboundHandlerAdapter with ChannelOutboundHandler {

   val transport: NettyTransport

   def bind(ctx: ChannelHandlerContext, localAddress: SocketAddress, promise: ChannelPromise): Unit = ctx.bind(localAddress, promise)

   def connect(ctx: ChannelHandlerContext, remoteAddress: SocketAddress, localAddress: SocketAddress, promise: ChannelPromise): Unit = ctx.connect(remoteAddress, localAddress, promise)

   def disconnect(ctx: ChannelHandlerContext, promise: ChannelPromise): Unit = ctx.disconnect(promise)

   def close(ctx: ChannelHandlerContext, promise: ChannelPromise): Unit = ctx.close(promise)

   def deregister(ctx: ChannelHandlerContext, promise: ChannelPromise): Unit = ctx.deregister(promise)

   def read(ctx: ChannelHandlerContext): Unit = ctx.read()

   override def channelRead(ctx: ChannelHandlerContext, msg: scala.Any): Unit = {
      transport.updateTotalBytesRead(msg.asInstanceOf[ByteBuf].readableBytes())
      super.channelRead(ctx, msg)
   }

   override def channelActive(ctx: ChannelHandlerContext) {
      transport.acceptedChannels.add(ctx.channel)
      super.channelActive(ctx)
   }

   override def write(ctx: ChannelHandlerContext, msg: scala.Any, promise: ChannelPromise): Unit = {
      val readable = msg.asInstanceOf[ByteBuf].readableBytes()
      ctx.write(msg, promise.addListener(new ChannelFutureListener {
         def operationComplete(future: ChannelFuture): Unit = {
            if (future.isSuccess) {
               transport.updateTotalBytesWritten(readable)
            }
         }
      }))
   }

   def flush(ctx: ChannelHandlerContext): Unit = ctx.flush()

}
