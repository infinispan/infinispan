package org.infinispan.server.resp.commands.generic;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.commons.util.concurrent.CompletionStages;
import org.infinispan.multimap.impl.EmbeddedMultimapListCache;
import org.infinispan.multimap.impl.EmbeddedMultimapPairCache;
import org.infinispan.multimap.impl.ScoredValue;
import org.infinispan.multimap.impl.SortableBucket;
import org.infinispan.multimap.impl.internal.MultimapObjectWrapper;
import org.infinispan.server.resp.AclCategory;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.RespUtil;
import org.infinispan.server.resp.commands.LimitArgument;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.serialization.ResponseWriter;

import io.netty.channel.ChannelHandlerContext;

/**
 * Returns or stores the elements contained in the list, set or sorted set at key.
 *
 * There is also the {@link SORT_RO} read-only variant of this command.
 *
 * @since 15.0
 * @see <a href="https://redis.io/commands/sort/">SORT</a>
 */
public class SORT extends RespCommand implements Resp3Command {
   private static final char REPLACEMENT = '*';
   private static final String VALUE_PATTERN = "#";
   private static final String HASH_FIELD = "->";
   private boolean readonly = false;

   public enum Arg {
      ALPHA, ASC, DESC, LIMIT, STORE, BY, GET
   }

   public SORT() {
      super(-2, 1, 1, 1);
   }

   @Override
   public long aclMask() {
      return AclCategory.WRITE | AclCategory.SET | AclCategory.SORTEDSET | AclCategory.LIST | AclCategory.SLOW | AclCategory.DANGEROUS;
   }

   public void disableStore() {
      this.readonly = true;
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler,
                                                      ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {

      int pos = 0;
      byte[] key = arguments.get(pos++);
      final SortableBucket.SortOptions sortOptions = new SortableBucket.SortOptions();
      byte[] destination = null;
      String pattern = null;
      List<String> getObjectsPatterns = new ArrayList<>();
      while (pos < arguments.size()) {
         switch (SORT.Arg.valueOf(new String(arguments.get(pos++)).toUpperCase())) {
            case ALPHA:
               sortOptions.alpha = true;
               break;
            case ASC:
               sortOptions.asc = true;
               break;
            case DESC:
               sortOptions.asc = false;
               break;
            case STORE:
               if (readonly) {
                  handler.writer().syntaxError();
                  return handler.myStage();
               }
               if (pos >= arguments.size()) {
                  handler.writer().syntaxError();
                  return handler.myStage();
               }
               destination = arguments.get(pos++);
               break;
            case BY:
               if (pos >= arguments.size()) {
                  handler.writer().syntaxError();
                  return handler.myStage();
               }
               pattern = new String(arguments.get(pos++));
               break;
            case LIMIT: {
               LimitArgument limitArgument = LimitArgument.parse(handler, arguments, pos);
               if (limitArgument.error){
                  return handler.myStage();
               }
               sortOptions.offset = limitArgument.offset;
               sortOptions.count = limitArgument.count;
               pos = limitArgument.nextArgPos;
               break;
            }
            case GET: {
               if (pos >= arguments.size()) {
                  handler.writer().syntaxError();
                  return handler.myStage();
               }
               getObjectsPatterns.add(new String(arguments.get(pos++)));
               break;
            }
            default:
               handler.writer().syntaxError();
               return handler.myStage();
         }
      }

      CompletionStage<List<ScoredValue<byte[]>>> sortedCollection;
      MediaType vmt = handler.cache().getValueDataConversion().getStorageMediaType();
      AdvancedCache<byte[], ?> cache = handler.typedCache(vmt);
      if (pattern == null || !pattern.contains("*")) {
         sortOptions.skipSort = pattern != null;
         sortedCollection = cache.getCacheEntryAsync(key)
               .thenApply(e -> {
                  if (e == null) return Collections.emptyList();

                  if (!(e.getValue() instanceof SortableBucket<?>))
                     throw new ClassCastException();

                  @SuppressWarnings("unchecked")
                  SortableBucket<byte[]> sb = (SortableBucket<byte[]>) e.getValue();
                  return sb.sort(sortOptions);
               });
      } else {
         String finalPattern = pattern;
         sortedCollection = cache.getCacheEntryAsync(key)
               .thenCompose(entry -> {
                  if (entry == null)
                     return CompletableFuture.completedFuture(Collections.emptyList());

                  if (!(entry.getValue() instanceof SortableBucket<?>))
                     throw new ClassCastException();

                  @SuppressWarnings("unchecked")
                  SortableBucket<byte[]> sb = (SortableBucket<byte[]>) entry.getValue();
                  return retrieveExternal(handler, sb, finalPattern, sortOptions);
               });
      }

      if (!getObjectsPatterns.isEmpty()) {
         CompletionStage<List<byte[]>> resultingList = sortedCollection
               .thenCompose(collection -> retrieveExternal(handler, collection, getObjectsPatterns));

         if (destination != null) {
            return storeV(handler, ctx, destination, resultingList);
         }

         return handler.stageToReturn(resultingList, ctx, ResponseWriter.ARRAY_BULK_STRING);
      }

      // STORE
      if (destination != null) {
         return store(handler, ctx, destination, sortedCollection);
      }

      CompletionStage<Collection<byte[]>> cs = sortedCollection
            .thenApply(res -> res.stream().map(ScoredValue::getValue).toList());
      return handler.stageToReturn(cs, ctx, ResponseWriter.ARRAY_BULK_STRING);
   }

   private static byte[] computePatternKey(String pattern, int index, byte[] value) {
      String computedKey =
            pattern.substring(0, index) + RespUtil.ascii(value) + pattern.substring(index + 1);
      return computedKey.getBytes(StandardCharsets.US_ASCII);
   }

   private CompletionStage<RespRequestHandler> storeV(Resp3Handler handler,
                                                     ChannelHandlerContext ctx,
                                                     byte[] destination,
                                                     CompletionStage<List<byte[]>> resultingList) {
      EmbeddedMultimapListCache<byte[], byte[]> listMultimap = handler.getListMultimap();
      CompletionStage<Long> cs = resultingList.thenCompose(values -> listMultimap.replace(destination, values));
      return handler.stageToReturn(cs, ctx, ResponseWriter.INTEGER);
   }

   private CompletionStage<RespRequestHandler> store(Resp3Handler handler,
                                                     ChannelHandlerContext ctx,
                                                     byte[] destination,
                                                     CompletionStage<List<ScoredValue<byte[]>>> sortedList) {
      EmbeddedMultimapListCache<byte[], byte[]> listMultimap = handler.getListMultimap();
      CompletionStage<Long> cs = sortedList.thenCompose(values ->
            listMultimap.replace(destination, values.stream().map(ScoredValue::getValue).collect(Collectors.toList())));
      return handler.stageToReturn(cs, ctx, ResponseWriter.INTEGER);
   }

   private CompletionStage<List<ScoredValue<byte[]>>> retrieveExternal(Resp3Handler handler, SortableBucket<byte[]> bucket, String pattern, SortableBucket.SortOptions sortOptions) {
      int index = pattern.indexOf(REPLACEMENT);
      var cs = CompletionStages.performSequentially(bucket.stream().iterator(),
            wv -> readScoredValue(handler, wv, pattern, index),
            Collectors.toList());
      return cs.thenApply(list -> bucket.sort(list.stream(), sortOptions));
   }

   private CompletionStage<ScoredValue<byte[]>> readScoredValue(Resp3Handler handler, MultimapObjectWrapper<byte[]> obj, String pattern, int index) {
      CompletionStage<byte[]> cs = getEntryValue(handler, pattern, index, obj.get());
      return cs.thenApply(w -> {
         if (w == null) {
            return new ScoredValue<>(1d, obj);
         }
         MultimapObjectWrapper<byte[]> wrappedWeight = new MultimapObjectWrapper<>(w);
         return new ScoredValue<>(wrappedWeight.asDouble(), obj);
      });
   }

   private CompletionStage<List<byte[]>> retrieveExternal(Resp3Handler handler, List<ScoredValue<byte[]>> collection, Collection<String> patterns) {
      var cs = CompletionStages.performSequentially(patterns.iterator(), pattern -> {
         if (pattern.equals(VALUE_PATTERN)) {
            return CompletableFuture.completedFuture(
                  collection.stream().map(ScoredValue::getValue).toList());
         }

         int index = pattern.indexOf(REPLACEMENT);
         if (index < 0) {
            return CompletableFuture.completedFuture(
                  Stream.<byte[]>generate(() -> null).limit(collection.size()).toList());
         }

         return CompletionStages.performSequentially(collection.iterator(),
               s -> getEntryValue(handler, pattern, index, s.getValue()),
               Collectors.toList());
      }, Collectors.toList());

      return cs.thenApply(patternGetResults -> {
         if (patternGetResults.isEmpty()) {
            return Collections.emptyList();
         }
         int size = patternGetResults.get(0).size();
         List<byte[]> finalResult = new ArrayList<>(patternGetResults.size() * size);
         for (int i = 0; i< size; i++) {
            for (List<byte[]> current: patternGetResults) {
               finalResult.add(current.get(i));
            }
         }
         return finalResult;
      });
   }

   private CompletionStage<byte[]> getEntryValue(Resp3Handler handler, String pattern, int index, byte[] replacement) {
      if (pattern.contains(HASH_FIELD)) {
         String[] split = pattern.split(HASH_FIELD);
         byte[] hashKey = computePatternKey(split[0], index, replacement);
         byte[] hashField = split[1].getBytes(StandardCharsets.US_ASCII);

         EmbeddedMultimapPairCache<byte[], byte[], byte[]> hash = handler.getHashMapMultimap();
         return hash.get(hashKey, hashField);
      }

      try {
         AdvancedCache<byte[], ?> cache = handler.cache();
         return cache.getAsync(computePatternKey(pattern, index, replacement))
               .handle((v, t) -> {
                  if (t != null) {
                     t = CompletableFutures.extractException(t);
                     if (t instanceof ClassCastException)
                        return null;

                     throw CompletableFutures.asCompletionException(t);
                  }

                  if (!(v instanceof byte[] bytes))
                     return null;

                  return bytes;
               });
      } catch (ClassCastException ignore) {
         return CompletableFutures.completedNull();
      }
   }
}
