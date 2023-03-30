package org.infinispan.server.core;

import java.util.List;

import org.infinispan.server.core.logging.Log;
import org.infinispan.server.core.transport.AccessControlFilter;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;

public abstract class MagicByteDetector extends ProtocolDetector {
   private final byte magicByte;

   protected MagicByteDetector(AbstractProtocolServer<?> server, byte magicByte) {
      super(server);
      this.magicByte = magicByte;
   }

   @Override
   protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
      // We need to only see the Magic byte
      if (in.readableBytes() < 1) {
         // noop, wait for further reads
         return;
      }
      byte b = in.getByte(in.readerIndex());
      if (b == magicByte) {
         // We found the Magic, let's do some pipeline surgery
         trimPipeline(ctx);
         // Add the protocol server handler
         ctx.pipeline().addLast(getInitializer());
         Log.SERVER.tracef("Detected %s connection %s", getName(), ctx);
         // Make sure to fire registered on the newly installed handlers
         ctx.fireChannelRegistered();
         // Trigger any protocol-specific rules
         ctx.pipeline().fireUserEventTriggered(AccessControlFilter.EVENT);
      }
      // Remove this
      ctx.pipeline().remove(this);
   }

   protected ChannelInitializer<Channel> getInitializer() {
      return server.getInitializer();
   }
}
