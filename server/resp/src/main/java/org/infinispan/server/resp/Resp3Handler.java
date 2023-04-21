package org.infinispan.server.resp;

import io.netty.channel.ChannelHandlerContext;
import org.infinispan.AdvancedCache;
import org.infinispan.context.Flag;
import org.infinispan.server.resp.commands.Resp3Command;

import java.util.List;
import java.util.concurrent.CompletionStage;

public class Resp3Handler extends Resp3AuthHandler {
   protected AdvancedCache<byte[], byte[]> ignorePreviousValueCache;

   Resp3Handler(RespServer respServer) {
      super(respServer);
   }

   @Override
   protected void setCache(AdvancedCache<byte[], byte[]> cache) {
      super.setCache(cache);
      ignorePreviousValueCache = cache.withFlags(Flag.SKIP_CACHE_LOAD, Flag.IGNORE_RETURN_VALUES);
   }

   @Override
   protected CompletionStage<RespRequestHandler> actualHandleRequest(ChannelHandlerContext ctx, RespCommand type, List<byte[]> arguments) {
      if (type instanceof Resp3Command) {
         Resp3Command resp3Command = (Resp3Command) type;
         return resp3Command.perform(this, ctx, arguments);
      }
      return super.actualHandleRequest(ctx, type, arguments);
   }

   protected static void handleLongResult(Long result, ByteBufPool alloc) {
      // TODO: this can be optimized to avoid the String allocation
      ByteBufferUtils.stringToByteBuf(":" + result + "\r\n", alloc);
   }

   protected static void handleThrowable(ByteBufPool alloc, Throwable t) {
      ByteBufferUtils.stringToByteBuf("-ERR " + t.getMessage() + "\r\n", alloc);
   }

   public AdvancedCache<byte[], byte[]> ignorePreviousValuesCache() {
      return ignorePreviousValueCache;
   }

   public CompletionStage<RespRequestHandler> delegate(ChannelHandlerContext ctx,
                                                       RespCommand command,
                                                       List<byte[]> arguments) {
      return super.actualHandleRequest(ctx, command, arguments);
   }
}
