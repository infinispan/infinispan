package org.infinispan.server.core;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import java.util.concurrent.Executor;

/**
 * Read only based handler that will execute any subsequent read operations on the executor provided
 */
@ChannelHandler.Sharable
public class ExecutionHandler extends ChannelInboundHandlerAdapter {

   private final Executor executor;

   /**
    * Creates a new instance with the specified {@link Executor}.
    */
   public ExecutionHandler(Executor executor) {
      if (executor == null) {
         throw new NullPointerException("executor");
      }
      this.executor = executor;
   }

   /**
    * Returns the {@link Executor} which was specified with the constructor.
    */
   public Executor getExecutor() {
      return executor;
   }

   @Override
   public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
      executor.execute(() -> ctx.fireChannelRead(msg));
   }
}
