package org.infinispan.server.resp;

import static org.infinispan.server.resp.RespConstants.CRLF_STRING;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.context.Flag;
import org.infinispan.multimap.impl.EmbeddedMultimapListCache;
import org.infinispan.security.AuthorizationManager;
import org.infinispan.security.AuthorizationPermission;
import org.infinispan.server.resp.commands.Resp3Command;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.ChannelHandlerContext;

public class Resp3Handler extends Resp3AuthHandler {
   protected AdvancedCache<byte[], byte[]> ignorePreviousValueCache;
   protected EmbeddedMultimapListCache<byte[], byte[]> listMultimap;

   private final MediaType valueMediaType;

   Resp3Handler(RespServer respServer, MediaType valueMediaType) {
      super(respServer);
      this.valueMediaType = valueMediaType;
   }

   @Override
   public void setCache(AdvancedCache<byte[], byte[]> cache) {
      super.setCache(cache);
      ignorePreviousValueCache = cache.withFlags(Flag.SKIP_CACHE_LOAD, Flag.IGNORE_RETURN_VALUES);
      listMultimap = new EmbeddedMultimapListCache<>(cache.withMediaType(MediaType.APPLICATION_OCTET_STREAM, valueMediaType));
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
      ByteBufferUtils.writeLong(result, alloc);
   }

   protected static void handleDoubleResult(double result, ByteBufPool alloc) {
      // TODO: this can be optimized to avoid the String allocation
      handleBulkResult(Double.toString(result), alloc);
   }

   protected static void handleCollectionLongResult(Collection<Long> collection, ByteBufPool alloc) {
      if (collection == null) {
         handleNullResult(alloc);
      } else {
         String result = "*" + collection.size() + CRLF_STRING + collection.stream().map(value -> ":" + value  + CRLF_STRING).collect(Collectors.joining());
         ByteBufferUtils.stringToByteBuf(result, alloc);
      }
   }

   public static void handleBulkResult(CharSequence result, ByteBufPool alloc) {
      if (result == null) {
         handleNullResult(alloc);
      }  else {
         ByteBufferUtils.stringToByteBuf("$" + ByteBufUtil.utf8Bytes(result) + CRLF_STRING + result + CRLF_STRING, alloc);
      }
   }

   private static void handleNullResult(ByteBufPool alloc) {
      ByteBufferUtils.stringToByteBuf("$-1\r\n", alloc);
   }

   protected static void handleBulkResult(byte[] result, ByteBufPool alloc) {
      var buffer = handleLengthPrefix('$', result.length, alloc, result.length + 2);
      buffer.writeBytes(result);
      buffer.writeByte('\r');
      buffer.writeByte('\n');
   }

   protected static void handleThrowable(ByteBufPool alloc, Throwable t) {
      ByteBufferUtils.stringToByteBuf("-ERR " + t.getMessage() + CRLF_STRING, alloc);
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

   protected static void handleArrayPrefix(int size, ByteBufPool alloc) {
      handleLengthPrefix('*', size, alloc, 0);
   }

   private static ByteBuf handleLengthPrefix(char type, int size, ByteBufPool alloc, int additionalBytes) {
      int strLength = size==0 ? 1 : (int)Math.log10(size) + 1;
      ByteBuf buffer = alloc.acquire(strLength + additionalBytes + 3);
      buffer.writeByte(type);
      ByteBufferUtils.setIntChars(size, strLength, buffer);
      buffer.writeByte('\r');
      buffer.writeByte('\n');
      return buffer;
   }
}
