package org.infinispan.server.memcached;

import static org.infinispan.server.core.transport.CacheInitializeInboundAdapter.CACHE_INITIALIZE_EVENT;

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
   private final MemcachedServer server;

   public MemcachedAutoDetector(MemcachedServer server) {
      super(server);
      this.server = server;
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

      ctx.pipeline().addLast(server.getInitializer(protocol));
      Log.SERVER.tracef("Memcached AUTO detected %s connection", protocol);
      // Trigger any protocol-specific rules
      ctx.pipeline().fireUserEventTriggered(AccessControlFilter.EVENT);
      ctx.pipeline().fireUserEventTriggered(CACHE_INITIALIZE_EVENT);
   }
}
