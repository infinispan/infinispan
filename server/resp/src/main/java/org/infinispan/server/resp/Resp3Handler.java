package org.infinispan.server.resp;

import static org.infinispan.server.resp.RespConstants.CRLF;
import static org.infinispan.server.resp.RespConstants.CRLF_STRING;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.context.Flag;
import org.infinispan.multimap.impl.EmbeddedMultimapListCache;
import org.infinispan.multimap.impl.EmbeddedMultimapPairCache;
import org.infinispan.multimap.impl.EmbeddedMultimapSortedSetCache;
import org.infinispan.multimap.impl.EmbeddedSetCache;
import org.infinispan.security.AuthorizationManager;
import org.infinispan.security.AuthorizationPermission;
import org.infinispan.server.resp.commands.Resp3Command;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.ChannelHandlerContext;

public class Resp3Handler extends Resp3AuthHandler {
   private static byte[] CRLF_BYTES = CRLF_STRING.getBytes();
   protected AdvancedCache<byte[], byte[]> ignorePreviousValueCache;
   protected EmbeddedMultimapListCache<byte[], byte[]> listMultimap;
   protected EmbeddedMultimapPairCache<byte[], byte[], byte[]> mapMultimap;
   protected EmbeddedSetCache<byte[], byte[]> embeddedSetCache;
   protected EmbeddedMultimapSortedSetCache<byte[], byte[]> sortedSetMultimap;

   private final MediaType valueMediaType;

   Resp3Handler(RespServer respServer, MediaType valueMediaType) {
      super(respServer);
      this.valueMediaType = valueMediaType;
   }

   @Override
   public void setCache(AdvancedCache<byte[], byte[]> cache) {
      super.setCache(cache);
      ignorePreviousValueCache = cache.withFlags(Flag.SKIP_CACHE_LOAD, Flag.IGNORE_RETURN_VALUES);
      Cache toMultimap = cache.withMediaType(MediaType.APPLICATION_OCTET_STREAM, valueMediaType);
      listMultimap = new EmbeddedMultimapListCache<>(toMultimap);
      mapMultimap = new EmbeddedMultimapPairCache<>(toMultimap);
      embeddedSetCache = new EmbeddedSetCache<>(toMultimap);
      sortedSetMultimap = new EmbeddedMultimapSortedSetCache<>(toMultimap);
   }

   public EmbeddedMultimapListCache<byte[], byte[]> getListMultimap() {
      return listMultimap;
   }

   public EmbeddedMultimapPairCache<byte[], byte[], byte[]> getHashMapMultimap() {
      return mapMultimap;
   }

   public EmbeddedSetCache<byte[], byte[]> getEmbeddedSetCache() {
      return embeddedSetCache;
   }

   public EmbeddedMultimapSortedSetCache<byte[], byte[]> getSortedSeMultimap() {
      return sortedSetMultimap;
   }

   @Override
   protected CompletionStage<RespRequestHandler> actualHandleRequest(ChannelHandlerContext ctx, RespCommand type,
         List<byte[]> arguments) {
      if (type instanceof Resp3Command) {
         Resp3Command resp3Command = (Resp3Command) type;
         return resp3Command.perform(this, ctx, arguments);
      }
      return super.actualHandleRequest(ctx, type, arguments);
   }

   protected static void handleLongResult(Long result, ByteBufPool alloc) {
      ByteBufferUtils.writeLong(result, alloc);
   }

   protected static void handleDoubleResult(Double result, ByteBufPool alloc) {
      // TODO: this can be optimized to avoid the String allocation
      if (result == null) {
         handleNullResult(alloc);
      } else {
         handleBulkAsciiResult(Double.toString(result), alloc);
      }
   }

   protected static void handleCollectionDoubleResult(Collection<Double> collection, ByteBufPool alloc) {
      if (collection == null) {
         handleNullResult(alloc);
      } else {
         writeArrayPrefix(collection.size(), alloc);
         for(Double d: collection) {
            if (d == null) {
               handleNullResult(alloc);
            } else{
               handleDoubleResult(d, alloc);
            }
         }
      }
   }

   protected static void handleCollectionLongResult(Collection<Long> collection, ByteBufPool alloc) {
      if (collection == null) {
         handleNullResult(alloc);
      } else {
         String result = "*" + collection.size() + CRLF_STRING
               + collection.stream().map(value -> ":" + value + CRLF_STRING).collect(Collectors.joining());
         ByteBufferUtils.stringToByteBufAscii(result, alloc);
      }
   }

   public static void handleBulkResult(CharSequence result, ByteBufPool alloc) {
      if (result == null) {
         handleNullResult(alloc);
      } else {
         int resultLength = ByteBufUtil.utf8Bytes(result);
         int resultSizeLength = ByteBufferUtils.stringSize(resultLength);
         ByteBuf buf = alloc.acquire(1 + resultSizeLength + 2 + resultLength + 2);
         buf.writeByte('$');
         ByteBufferUtils.setIntChars(resultLength, resultSizeLength, buf);
         buf.writeBytes(CRLF);
         ByteBufUtil.writeUtf8(buf, result);
         buf.writeBytes(CRLF);
      }
   }

   public static void handleBulkAsciiResult(CharSequence result, ByteBufPool alloc) {
      if (result == null) {
         handleNullResult(alloc);
      } else {
         int resultLength = result.length();
         int resultSizeLength = ByteBufferUtils.stringSize(resultLength);
         ByteBuf buf = alloc.acquire(1 + resultSizeLength + 2 + resultLength + 2);
         buf.writeByte('$');
         ByteBufferUtils.setIntChars(resultLength, resultSizeLength, buf);
         buf.writeBytes(CRLF);
         ByteBufUtil.writeAscii(buf, result);
         buf.writeBytes(CRLF);
      }
   }

   public static void handleCollectionBulkResult(Collection<byte[]> collection, ByteBufPool alloc) {
      if (collection == null) {
         handleNullResult(alloc);
         return;
      }
      int dataLength = collection.stream().mapToInt(ba -> ba.length + 5 + lenghtInChars(ba.length)).sum();
      var buffer = allocAndWriteLengthPrefix('*', collection.size(), alloc, dataLength);
      collection.forEach(wba -> writeBulkResult(wba, buffer));
   }

   private static void handleNullResult(ByteBufPool alloc) {
      ByteBufferUtils.stringToByteBufAscii("$-1\r\n", alloc);
   }

   protected static void handleBulkResult(byte[] result, ByteBufPool alloc) {
      if (result == null) {
         handleNullResult(alloc);
         return;
      }
      var buffer = allocAndWriteLengthPrefix('$', result.length, alloc, result.length + 2);
      buffer.writeBytes(result);
      buffer.writeBytes(CRLF_BYTES);
   }

   private static void writeBulkResult(byte[] result, ByteBuf buffer) {
      writeLengthPrefix('$', result.length, buffer);
      buffer.writeBytes(result);
      buffer.writeBytes(CRLF_BYTES);
   }

   protected static void handleThrowable(ByteBufPool alloc, Throwable t) {
      Consumer<ByteBufPool> writer = RespErrorUtil.handleException(t);
      if (writer != null) {
         writer.accept(alloc);
      } else {
         ByteBufferUtils.stringToByteBuf("-ERR " + t.getMessage() + CRLF_STRING, alloc);
      }
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

   public static void writeArrayPrefix(int size, ByteBufPool alloc) {
      allocAndWriteLengthPrefix('*', size, alloc, 0);
   }

   private static ByteBuf allocAndWriteLengthPrefix(char type, int size, ByteBufPool alloc, int additionalBytes) {
      int strLength = lenghtInChars(size);
      ByteBuf buffer = alloc.acquire(strLength + additionalBytes + 3);
      buffer.writeByte(type);
      ByteBufferUtils.setIntChars(size, strLength, buffer);
      buffer.writeBytes(CRLF_BYTES);
      return buffer;
   }

   private static int lenghtInChars(int size) {
      int strLength = size == 0 ? 1 : (int) Math.log10(size) + 1;
      return strLength;
   }

   private static ByteBuf writeLengthPrefix(char type, int size, ByteBuf buffer) {
      int strLength = lenghtInChars(size);
      buffer.writeByte(type);
      ByteBufferUtils.setIntChars(size, strLength, buffer);
      buffer.writeBytes(CRLF_BYTES);
      return buffer;
   }

}
