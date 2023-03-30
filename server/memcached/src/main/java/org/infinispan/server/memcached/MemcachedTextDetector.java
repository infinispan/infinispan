package org.infinispan.server.memcached;

import java.nio.charset.StandardCharsets;
import java.util.List;

import org.infinispan.server.core.ProtocolDetector;
import org.infinispan.server.core.logging.Log;
import org.infinispan.server.core.transport.AccessControlFilter;
import org.infinispan.server.memcached.configuration.MemcachedProtocol;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;

/**
 * Detect Memcached connections
 */
public class MemcachedTextDetector extends ProtocolDetector {
   public static final String NAME = "memcached-text-detector";
   private final MemcachedServer server;

   public MemcachedTextDetector(MemcachedServer server) {
      super(server);
      this.server = server;
   }

   @Override
   public String getName() {
      return NAME;
   }

   @Override
   protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
      // We need to only see the SET command
      if (in.readableBytes() < 4) {
         // noop, wait for further reads
         return;
      }
      int i = in.readerIndex();
      // Memcached authentication is performed via a "fake" set command
      CharSequence handshake = in.getCharSequence(i, 4, StandardCharsets.US_ASCII);
      if ("set ".contentEquals(handshake)) {
         installHandler(ctx);
      }
      // Remove this
      ctx.pipeline().remove(this);
   }

   private void installHandler(ChannelHandlerContext ctx) {
      // We found the Memcached authentication command, let's do some pipeline surgery
      trimPipeline(ctx);
      // Add the Memcached server handler
      ctx.pipeline().addLast(server.getInitializer(MemcachedProtocol.TEXT));
      Log.SERVER.tracef("Detected Memcached text connection %s", ctx);
      // Trigger any protocol-specific rules
      ctx.pipeline().fireUserEventTriggered(AccessControlFilter.EVENT);
   }
}
