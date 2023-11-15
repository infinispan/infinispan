package org.infinispan.server.resp.commands.generic;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.multimap.impl.EmbeddedMultimapListCache;
import org.infinispan.multimap.impl.ScoredValue;
import org.infinispan.multimap.impl.SortableBucket;
import org.infinispan.multimap.impl.internal.MultimapObjectWrapper;
import org.infinispan.server.resp.Consumers;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespErrorUtil;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.LimitArgument;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.util.concurrent.CompletionStages;

import io.netty.channel.ChannelHandlerContext;

/**
 * Returns or stores the elements contained in the list, set or sorted set at key.
 *
 * There is also the {@link SORT_RO} read-only variant of this command.
 *
 * @since 15.0
 * @see <a href="https://redis.io/commands/sort/">Redis Documentation</a>
 */
public class SORT extends RespCommand implements Resp3Command {
   public static final char REPLACEMENT = '*';
   public static final String VALUE_PATTERN = "#";
   private boolean readonly = false;

   public enum Arg {
      ALPHA, ASC, DESC, LIMIT, STORE, BY, GET
   }

   public SORT() {
      super(-2, 1, 1, 1);
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
                  RespErrorUtil.syntaxError(handler.allocator());
                  return handler.myStage();
               }
               if (pos >= arguments.size()) {
                  RespErrorUtil.syntaxError(handler.allocator());
                  return handler.myStage();
               }
               destination = arguments.get(pos++);
               break;
            case BY:
               if (pos >= arguments.size()) {
                  RespErrorUtil.syntaxError(handler.allocator());
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
                  RespErrorUtil.syntaxError(handler.allocator());
                  return handler.myStage();
               }
               getObjectsPatterns.add(new String(arguments.get(pos++)));
               break;
            }
            default:
               RespErrorUtil.syntaxError(handler.allocator());
               return handler.myStage();
         }
      }
      if ("nosort".equals(pattern) || (pattern != null && !pattern.contains("*"))) {
         sortOptions.skipSort = true;
      }

      CompletionStage<List<ScoredValue<byte[]>>> sortedCollection;
      MediaType vmt = handler.cache().getValueDataConversion().getStorageMediaType();
      AdvancedCache<byte[], byte[]> cache = handler.cache().withMediaType(MediaType.APPLICATION_OCTET_STREAM, vmt);
      if (pattern == null || sortOptions.skipSort) {
         sortedCollection = cache.getCacheEntryAsync(key).thenApply(e -> {
                  if (e == null) {
                     return Collections.emptyList();
                  }
                  Object value = e.getValue();
                  if (value instanceof SortableBucket) {
                     SortableBucket<byte[]> sortableBucket = (SortableBucket) value;
                     return sortableBucket.sort(sortOptions);
                  }
                  throw new ClassCastException();
               });
      } else {
         final String finalPattern = pattern;
         int index = pattern.indexOf(REPLACEMENT);
         sortedCollection = CompletionStages.handleAndCompose(cache.getCacheEntryAsync(key),
               (e, t) -> {
                  if (e == null) {
                     return CompletableFuture.completedFuture(Collections.emptyList());
                  }
                  Object value = e.getValue();
                  if (value instanceof SortableBucket) {
                     SortableBucket<byte[]> sortableBucket = (SortableBucket) value;
                     return CompletableFutures.sequence(
                           sortableBucket.stream().map(wrappedValue -> {
                              byte[] computedWeightKey = computePatternKey(finalPattern, index, wrappedValue.get());
                              return handler.cache().getAsync(computedWeightKey).thenApply(w -> {
                                 if (w == null) {
                                    return new ScoredValue<>(1d, wrappedValue);
                                 }
                                 MultimapObjectWrapper<byte[]> wrappedWeight = new MultimapObjectWrapper<>(w);
                                 return new ScoredValue<>(wrappedWeight.asDouble(), wrappedValue);
                              });
                           }).collect(Collectors.toList()))
                           .thenApply(list -> sortableBucket.sort(list.stream(), sortOptions));
                  }
                  throw new ClassCastException();
               });
      }

      if (!getObjectsPatterns.isEmpty()) {
         CompletionStage<List<byte[]>> resultingList = CompletionStages.handleAndCompose(sortedCollection,
                     (collection, t) -> CompletableFutures.sequence(getObjectsPatterns.stream().map(getPattern -> {
                        if (getPattern.equals(VALUE_PATTERN)) {
                           return CompletableFuture.completedFuture(
                                 collection.stream().map(sv -> sv.getValue()).collect(Collectors.toList()));
                        }

                        int index = getPattern.indexOf(REPLACEMENT);
                        if (index < 0) {
                           return CompletableFuture.completedFuture(
                                 Stream.<byte[]>generate(() -> null)
                                       .limit(collection.size()).collect(Collectors.toList()));
                        }

                        return CompletableFutures.sequence(collection.stream()
                              .map(s -> handler.cache().getCacheEntryAsync(computePatternKey(getPattern, index, s.getValue()))
                                    .thenApply(v -> {
                                       if (v == null || v.getValue().getClass() != byte[].class) {
                                          return null;
                                       }
                                       return v.getValue();
                                    })).collect(Collectors.toList()));
                     }).collect(Collectors.toList())))
               .thenApply(patternGetResults -> {
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
         if (destination != null) {
            return storeV(handler, ctx, destination, resultingList);
         }

         return handler.stageToReturn(resultingList, ctx, Consumers.GET_ARRAY_BICONSUMER);
      }

      // STORE
      if (destination != null) {
         return store(handler, ctx, destination, sortedCollection);
      }

      return handler.stageToReturn(sortedCollection, ctx, Consumers.GET_OBJ_WRAPPER_ARRAY_BICONSUMER);
   }

   private static byte[] computePatternKey(String pattern, int index, byte[] value) {
      String computedKey =
            pattern.substring(0, index) + new String(value) + pattern.substring(
                  index + 1);
      return computedKey.getBytes();
   }

   private CompletionStage<RespRequestHandler> storeV(Resp3Handler handler,
                                                     ChannelHandlerContext ctx,
                                                     byte[] destination,
                                                     CompletionStage<List<byte[]>> resultingList) {
      EmbeddedMultimapListCache<byte[], byte[]> listMultimap = handler.getListMultimap();
      CompletionStage<Long> cs = resultingList.thenCompose(values -> listMultimap.replace(destination, values));
      return handler.stageToReturn(cs, ctx, Consumers.LONG_BICONSUMER);
   }

   private CompletionStage<RespRequestHandler> store(Resp3Handler handler,
                                                     ChannelHandlerContext ctx,
                                                     byte[] destination,
                                                     CompletionStage<List<ScoredValue<byte[]>>> sortedList) {
      EmbeddedMultimapListCache<byte[], byte[]> listMultimap = handler.getListMultimap();
      CompletionStage<Long> cs = sortedList.thenCompose(values ->
            listMultimap.replace(destination, values.stream().map(ScoredValue::getValue).collect(Collectors.toList())));
      return handler.stageToReturn(cs, ctx, Consumers.LONG_BICONSUMER);
   }
}
