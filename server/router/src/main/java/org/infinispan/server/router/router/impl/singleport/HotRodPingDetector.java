package org.infinispan.server.router.router.impl.singleport;

import java.util.List;

import org.infinispan.server.hotrod.HotRodServer;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

public class HotRodPingDetector extends ByteToMessageDecoder {
   public static final String NAME = "hotrod-ping-detector";
   private static final byte HOTROD_MAGIC = (byte) 0xA0;
   private final HotRodServer hotRodServer;

   public HotRodPingDetector(HotRodServer hotRodServer) {
      this.hotRodServer = hotRodServer;
   }

   @Override
   protected  void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
      // We need to only see the Hot Rod Magic byte
      if (in.readableBytes() < 1) {
         // noop, wait for further reads
         return;
      }
      byte b = in.getByte(in.readerIndex());
      if (b == HOTROD_MAGIC) {
         // We found the Hot Rod Magic, let's do some pipeline surgery
         ChannelHandlerAdapter dummyHandler = new ChannelHandlerAdapter() {};
         ctx.pipeline().addAfter(NAME, "dummy", dummyHandler);
         ChannelHandler channelHandler = ctx.pipeline().removeLast();
         // Remove everything else
         while (channelHandler != dummyHandler) {
            channelHandler = ctx.pipeline().removeLast();
         }
         // Add the Hot Rod server handler
         ctx.pipeline().addLast(hotRodServer.getInitializer());
         // Remove this
         ctx.pipeline().remove(this);
      } else {
         // This ain't Hot Rod, remove ourselves and process as normal
         ctx.pipeline().remove(this);
      }
   }
}
