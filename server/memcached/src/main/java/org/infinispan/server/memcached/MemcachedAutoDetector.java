package org.infinispan.server.memcached;

import java.util.List;

import org.infinispan.server.core.ProtocolDetector;
import org.infinispan.server.core.logging.Log;
import org.infinispan.server.core.transport.AccessControlFilter;
import org.infinispan.server.memcached.binary.BinaryConstants;
import org.infinispan.server.memcached.configuration.MemcachedProtocol;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;

public class MemcachedAutoDetector extends ProtocolDetector {
   public static final String NAME = "memcached-auto-detector";

   public MemcachedAutoDetector(MemcachedServer server) {
      super(server);
   }

   @Override
   public String getName() {
      return NAME;
   }

   @Override
   protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
      // We need to only see the magic byte
      if (in.readableBytes() < 1) {
         // noop, wait for further reads
         return;
      }
      byte b = in.getByte(in.readerIndex());
      trimPipeline(ctx);
      MemcachedProtocol protocol = b == BinaryConstants.MAGIC_REQ ? MemcachedProtocol.BINARY : MemcachedProtocol.TEXT;

      MemcachedBaseDecoder decoder = (MemcachedBaseDecoder) ((MemcachedServer) server).getDecoder(protocol);
      ctx.pipeline().replace(this, "decoder", decoder);
      ((MemcachedServer) server).installMemcachedInboundHandler(ctx.channel(), decoder);
      // Make sure to fire registered on the newly installed handlers
      ctx.fireChannelRegistered();
      Log.SERVER.tracef("Detected %s connection", protocol);
      // Trigger any protocol-specific rules
      ctx.pipeline().fireUserEventTriggered(AccessControlFilter.EVENT);
   }
}
