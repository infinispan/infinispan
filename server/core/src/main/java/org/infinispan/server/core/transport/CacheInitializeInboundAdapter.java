package org.infinispan.server.core.transport;

import org.infinispan.server.core.AbstractProtocolServer;
import org.infinispan.server.core.logging.Log;
import org.infinispan.util.logging.LogFactory;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

public class CacheInitializeInboundAdapter extends ChannelInboundHandlerAdapter {
   private static final Log log = LogFactory.getLog(CacheInitializeInboundAdapter.class, Log.class);
   public static final Object CACHE_INITIALIZE_EVENT = new Object();

   private final AbstractProtocolServer<?> server;

   public CacheInitializeInboundAdapter(AbstractProtocolServer<?> server) {
      this.server = server;
   }

   private void initializeHandlerState(ChannelHandlerContext ctx, Runnable done) {
      if (server.isDefaultCacheInitialized() && server.isDefaultCacheRunning()) {
         done.run();
         remove(ctx);
         return;
      }

      ctx.channel().config().setAutoRead(false);
      server.initializeDefaultCache().whenCompleteAsync((ignore, t) -> {
         if (t != null) {
            log.errorf(t, "Failed to initialize default cache %s on channel %s", server.getName(), ctx.channel());
            ctx.close();
            return;
         }

         ctx.channel().config().setAutoRead(true);
         done.run();
         remove(ctx);
      }, ctx.channel().eventLoop());
   }

   private void remove(ChannelHandlerContext ctx) {
      ctx.pipeline().remove(this);
   }

   @Override
   public void channelActive(ChannelHandlerContext ctx) throws Exception {
      if (server.isDefaultCacheInitialized() && server.isDefaultCacheRunning()) {
         super.channelActive(ctx);
         remove(ctx);
         return;
      }

      initializeHandlerState(ctx, ctx::fireChannelActive);
   }

   @Override
   public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
      if (evt == CACHE_INITIALIZE_EVENT) {
         initializeHandlerState(ctx, () -> ctx.fireUserEventTriggered(CACHE_INITIALIZE_EVENT));
         return;
      }
      super.userEventTriggered(ctx, evt);
   }
}
