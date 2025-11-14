package org.infinispan.server.resp.commands.string;

import static org.infinispan.server.resp.operation.SwitchDbOperation.switchDB;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.multimap.impl.EmbeddedMultimapListCache;
import org.infinispan.multimap.impl.EmbeddedMultimapPairCache;
import org.infinispan.multimap.impl.EmbeddedMultimapSortedSetCache;
import org.infinispan.multimap.impl.EmbeddedSetCache;
import org.infinispan.multimap.impl.HashMapBucket;
import org.infinispan.multimap.impl.ListBucket;
import org.infinispan.multimap.impl.ScoredValue;
import org.infinispan.multimap.impl.SetBucket;
import org.infinispan.multimap.impl.SortedSetAddArgs;
import org.infinispan.multimap.impl.SortedSetBucket;
import org.infinispan.server.resp.AclCategory;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.RespServer;
import org.infinispan.server.resp.RespTypes;
import org.infinispan.server.resp.RespUtil;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.json.EmbeddedJsonCache;
import org.infinispan.server.resp.json.JsonBucket;
import org.infinispan.server.resp.logging.Log;
import org.infinispan.server.resp.serialization.ResponseWriter;

import io.netty.channel.ChannelHandlerContext;

/**
 * COPY
 *
 * @see <a href="https://redis.io/commands/copy/">COPY</a>
 * @since 16.1
 */
public class COPY extends RespCommand implements Resp3Command {
   public static final byte[] REPLACE_BYTES = "REPLACE".getBytes(StandardCharsets.US_ASCII);
   public static final byte[] DB_BYTES = "DB".getBytes(StandardCharsets.US_ASCII);

   private static final Log log = Log.getLog(RespServer.class);

   public COPY() {
      super(-2, 1, 1, 1, AclCategory.WRITE.mask() | AclCategory.KEYSPACE.mask() | AclCategory.SLOW.mask());
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler, ChannelHandlerContext ctx, List<byte[]> arguments) {
      if (arguments.size() < 2) {
         return CompletableFuture.failedFuture(new IllegalStateException("Missing arguments"));
      }

      boolean tmpIsReplace = false;
      String tmpDbName = null;
      for (int i = 2; i < arguments.size(); i++) {
         byte[] arg = arguments.get(i);
         if (RespUtil.isAsciiBytesEquals(DB_BYTES, arg)) {
            if (i + 1 == arguments.size()) throw new IllegalArgumentException("No database name provided.");
            tmpDbName = new String(arguments.get(++i), StandardCharsets.US_ASCII);
            if (!handler.respServer().getCacheManager().cacheExists(tmpDbName)) {
               throw new IllegalArgumentException("DB index is out of range");
            }
         } else if (RespUtil.isAsciiBytesEquals(REPLACE_BYTES, arg)) {
            tmpIsReplace = true;
         } else {
            throw new IllegalArgumentException("Unknown argument for COPY operation");
         }
      }

      String targetDb = tmpDbName;
      boolean isReplace = tmpIsReplace;
      AdvancedCache<byte[], byte[]> originalCache = handler.cache().getAdvancedCache();
      byte[] srcKey = arguments.get(0);
      byte[] dstKey = arguments.get(1);
      MediaType vmt = originalCache.getValueDataConversion().getStorageMediaType();

      return handler.stageToReturn(
            originalCache.withMediaType(MediaType.APPLICATION_OCTET_STREAM, vmt)
                  .getCacheEntryAsync(srcKey)
                  .thenCompose(e -> {
                     if (e == null) {
                        return CompletableFuture.completedFuture(0);
                     }

                     Object value = e.getValue();
                     long lifespan = e.getLifespan() > 0 ? e.getLifespan() : -1;
                     Cache<byte[], byte[]> targetCache = targetDb != null
                           ? handler.respServer().getCacheManager().getCache(targetDb)
                           : originalCache;
                     RespTypes type = RespTypes.fromValueClass(value.getClass());

                     if (type == RespTypes.json) {
                        return copyJson(handler, ctx, dstKey, value, isReplace, targetDb, originalCache.getName());
                     } else if (type == RespTypes.hash) {
                        return copyHash(handler, ctx, targetCache, vmt, dstKey, value, isReplace, targetDb, originalCache.getName());
                     } else if (type == RespTypes.list) {
                        return copyList(handler, ctx, targetCache, vmt, dstKey, value, isReplace, targetDb, originalCache.getName());
                     } else if (type == RespTypes.set) {
                        return copySet(handler, ctx, targetCache, vmt, dstKey, value, isReplace, targetDb, originalCache.getName());
                     } else if (type == RespTypes.zset) {
                        return copySortedSet(handler, ctx, targetCache, vmt, dstKey, value, isReplace, targetDb, originalCache.getName());
                     } else if (type == RespTypes.string) {
                        return copyString(targetCache, vmt, dstKey, (byte[]) value, lifespan, isReplace);
                     } else {
                        throw new IllegalArgumentException("unsupported type: " + value.getClass());
                     }
                  }),
            ctx, ResponseWriter.INTEGER);
   }

   private CompletionStage<Integer> copyJson(Resp3Handler handler, ChannelHandlerContext ctx,
         byte[] dstKey, Object value, boolean isReplace, String targetDb, String originalCacheName) {
      EmbeddedJsonCache ejc = handler.getJsonCache();
      if (targetDb != null) {
         switchDB(handler, targetDb, ctx);
         ejc = handler.getJsonCache();
      }
      return ejc.set(dstKey, ((JsonBucket) value).value(), "$".getBytes(), !isReplace, false)
            .thenApply(result -> {
               switchBackIfNeeded(handler, ctx, targetDb, originalCacheName);
               return "OK".equals(result) ? 1 : 0;
            });
   }

   private CompletionStage<Integer> copyString(Cache<byte[], byte[]> targetCache, MediaType vmt,
         byte[] dstKey, byte[] value, long lifespan, boolean isReplace) {
      AdvancedCache<byte[], Object> typedCache = targetCache.getAdvancedCache()
            .<byte[], Object>withMediaType(MediaType.APPLICATION_OCTET_STREAM, vmt);

      if (!isReplace && typedCache.containsKey(dstKey)) {
         return CompletableFuture.completedFuture(0);
      }

      CompletionStage<?> result;
      if (isReplace) {
         result = typedCache.removeAsync(dstKey)
               .thenCompose(v -> targetCache.putAsync(dstKey, value, lifespan, TimeUnit.MILLISECONDS));
      } else {
         result = targetCache.putAsync(dstKey, value, lifespan, TimeUnit.MILLISECONDS);
      }
      return result.thenApply(r -> 1);
   }

   @SuppressWarnings("unchecked")
   private CompletionStage<Integer> copyHash(Resp3Handler handler, ChannelHandlerContext ctx,
         Cache<byte[], byte[]> targetCache, MediaType vmt, byte[] dstKey, Object value,
         boolean isReplace, String targetDb, String originalCacheName) {
      switchToTargetDb(handler, ctx, targetDb);
      EmbeddedMultimapPairCache<byte[], byte[], byte[]> hashMap = handler.getHashMapMultimap();
      Map.Entry<byte[], byte[]>[] entries = ((HashMapBucket<byte[], byte[]>) value)
            .converted().entrySet().toArray(new Map.Entry[0]);
      return copyMultimapValue(handler, ctx, targetCache, vmt, dstKey, isReplace, targetDb, originalCacheName,
            () -> hashMap.set(dstKey, entries));
   }

   @SuppressWarnings("unchecked")
   private CompletionStage<Integer> copyList(Resp3Handler handler, ChannelHandlerContext ctx,
         Cache<byte[], byte[]> targetCache, MediaType vmt, byte[] dstKey, Object value,
         boolean isReplace, String targetDb, String originalCacheName) {
      switchToTargetDb(handler, ctx, targetDb);
      EmbeddedMultimapListCache<byte[], byte[]> listCache = handler.getListMultimap();
      List<byte[]> elements = new ArrayList<>(((ListBucket<byte[]>) value).toDeque());
      return copyMultimapValue(handler, ctx, targetCache, vmt, dstKey, isReplace, targetDb, originalCacheName,
            () -> listCache.offerLast(dstKey, elements));
   }

   @SuppressWarnings("unchecked")
   private CompletionStage<Integer> copySet(Resp3Handler handler, ChannelHandlerContext ctx,
         Cache<byte[], byte[]> targetCache, MediaType vmt, byte[] dstKey, Object value,
         boolean isReplace, String targetDb, String originalCacheName) {
      switchToTargetDb(handler, ctx, targetDb);
      EmbeddedSetCache<byte[], byte[]> setCache = handler.getEmbeddedSetCache();
      Collection<byte[]> elements = ((SetBucket<byte[]>) value).toSet();
      return copyMultimapValue(handler, ctx, targetCache, vmt, dstKey, isReplace, targetDb, originalCacheName,
            () -> setCache.set(dstKey, elements));
   }

   @SuppressWarnings("unchecked")
   private CompletionStage<Integer> copySortedSet(Resp3Handler handler, ChannelHandlerContext ctx,
         Cache<byte[], byte[]> targetCache, MediaType vmt, byte[] dstKey, Object value,
         boolean isReplace, String targetDb, String originalCacheName) {
      switchToTargetDb(handler, ctx, targetDb);
      EmbeddedMultimapSortedSetCache<byte[], byte[]> sortedSetCache = handler.getSortedSeMultimap();
      Collection<ScoredValue<byte[]>> scoredValues = ((SortedSetBucket<byte[]>) value).getScoredEntries();
      SortedSetAddArgs addArgs = SortedSetAddArgs.create().build();
      return copyMultimapValue(handler, ctx, targetCache, vmt, dstKey, isReplace, targetDb, originalCacheName,
            () -> sortedSetCache.addMany(dstKey, scoredValues, addArgs));
   }

   private CompletionStage<Integer> copyMultimapValue(Resp3Handler handler, ChannelHandlerContext ctx,
         Cache<byte[], byte[]> targetCache, MediaType vmt, byte[] dstKey, boolean isReplace,
         String targetDb, String originalCacheName, Supplier<CompletionStage<?>> writeOperation) {
      AdvancedCache<byte[], Object> typedCache = targetCache.getAdvancedCache()
            .<byte[], Object>withMediaType(MediaType.APPLICATION_OCTET_STREAM, vmt);

      if (!isReplace && typedCache.containsKey(dstKey)) {
         switchBackIfNeeded(handler, ctx, targetDb, originalCacheName);
         return CompletableFuture.completedFuture(0);
      }

      CompletionStage<?> result = isReplace
            ? typedCache.removeAsync(dstKey).thenCompose(v -> writeOperation.get())
            : writeOperation.get();

      return result.thenApply(r -> {
         switchBackIfNeeded(handler, ctx, targetDb, originalCacheName);
         return 1;
      });
   }

   private void switchToTargetDb(Resp3Handler handler, ChannelHandlerContext ctx, String targetDb) {
      if (targetDb != null) {
         switchDB(handler, targetDb, ctx);
      }
   }

   private void switchBackIfNeeded(Resp3Handler handler, ChannelHandlerContext ctx,
         String targetDb, String originalCacheName) {
      if (targetDb != null) {
         switchDB(handler, originalCacheName, ctx);
      }
   }
}
