package org.infinispan.server.core;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

/**
 * @since 15.0
 **/
public abstract class ProtocolDetector extends ByteToMessageDecoder {
   protected final ProtocolServer<?> server;

   protected ProtocolDetector(ProtocolServer<?> server) {
      this.server = server;
   }

   public abstract String getName();

   /**
    * Removes all handlers in the pipeline after this
    */
   protected void trimPipeline(ChannelHandlerContext ctx) {
      ChannelHandlerAdapter dummy = new ChannelHandlerAdapter() {};
      ctx.pipeline().addAfter(ctx.name(), "dummy", dummy);
      ChannelHandler channelHandler = ctx.pipeline().removeLast();
      // Remove everything else
      while (channelHandler != dummy) {
         channelHandler = ctx.pipeline().removeLast();
      }
   }
}
