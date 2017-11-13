package org.infinispan.client.hotrod.impl.transport.netty;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandler;

/**
 * This is effectively the same as {@link io.netty.channel.ChannelInboundHandlerAdapter} but allows
 * to be inherited in a class with another superclass.
 */
public interface ChannelInboundHandlerDefaults extends ChannelInboundHandler {
   @Override
   default void channelRegistered(ChannelHandlerContext ctx) throws Exception {
      ctx.fireChannelRegistered();
   }

   @Override
   default void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
      ctx.fireChannelUnregistered();
   }

   @Override
   default void channelActive(ChannelHandlerContext ctx) throws Exception {
      ctx.fireChannelActive();
   }

   @Override
   default void channelInactive(ChannelHandlerContext ctx) throws Exception {
      ctx.fireChannelInactive();
   }

   @Override
   default void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
      ctx.fireChannelRead(msg);
   }

   @Override
   default void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
      ctx.fireChannelReadComplete();
   }

   @Override
   default void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
      ctx.fireUserEventTriggered(evt);
   }

   @Override
   default void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
      ctx.fireChannelWritabilityChanged();
   }

   @Override
   default void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
      ctx.fireExceptionCaught(cause);
   }

   @Override
   default void handlerAdded(ChannelHandlerContext ctx) throws Exception {
      // noop
   }

   @Override
   default void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
      // noop
   }
}
