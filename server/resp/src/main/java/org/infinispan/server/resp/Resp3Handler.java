package org.infinispan.server.resp;

import static org.infinispan.server.resp.RespConstants.CRLF;

import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.AdvancedCache;
import org.infinispan.context.Flag;
import org.infinispan.multimap.impl.EmbeddedMultimapCacheManager;
import org.infinispan.multimap.impl.EmbeddedMultimapListCache;
import org.infinispan.security.AuthorizationManager;
import org.infinispan.security.AuthorizationPermission;
import org.infinispan.server.resp.commands.Resp3Command;

import io.netty.buffer.ByteBufUtil;
import io.netty.channel.ChannelHandlerContext;

public class Resp3Handler extends Resp3AuthHandler {
   protected AdvancedCache<byte[], byte[]> ignorePreviousValueCache;
   protected EmbeddedMultimapListCache<byte[], byte[]> listMultimap;

   Resp3Handler(RespServer respServer) {
      super(respServer);
   }

   @Override
   protected void setCache(AdvancedCache<byte[], byte[]> cache) {
      super.setCache(cache);
      ignorePreviousValueCache = cache.withFlags(Flag.SKIP_CACHE_LOAD, Flag.IGNORE_RETURN_VALUES);
      listMultimap = new EmbeddedMultimapCacheManager(cache.getCacheManager()).getMultimapList(cache.getName());
   }

   public EmbeddedMultimapListCache<byte[], byte[]> getListMultimap() {
      return listMultimap;
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
      ByteBufferUtils.stringToByteBuf(":" + result + CRLF, alloc);
   }

   protected static void handleDoubleResult(Double result, ByteBufPool alloc) {
      // TODO: this can be optimized to avoid the String allocation
      handleBulkResult(result.toString(), alloc);
   }

   public static void handleBulkResult(CharSequence result, ByteBufPool alloc) {
      if (result == null) {
         ByteBufferUtils.stringToByteBuf("$-1\r\n", alloc);
      }  else {
         ByteBufferUtils.stringToByteBuf("$" + ByteBufUtil.utf8Bytes(result) + CRLF + result + CRLF, alloc);
      }
   }

   protected static void handleThrowable(ByteBufPool alloc, Throwable t) {
      ByteBufferUtils.stringToByteBuf("-ERR " + t.getMessage() + CRLF, alloc);
   }

   public AdvancedCache<byte[], byte[]> ignorePreviousValuesCache() {
      return ignorePreviousValueCache;
   }

   public CompletionStage<RespRequestHandler> delegate(ChannelHandlerContext ctx,
                                                       RespCommand command,
                                                       List<byte[]> arguments) {
      return super.actualHandleRequest(ctx, command, arguments);
   }

   public void checkPermission(AuthorizationPermission authorizationPermission) {
      AuthorizationManager authorizationManager = cache.getAuthorizationManager();
      if (authorizationManager != null) {
         authorizationManager.checkPermission(authorizationPermission);
      }
   }
}
