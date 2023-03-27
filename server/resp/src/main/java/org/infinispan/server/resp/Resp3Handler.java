package org.infinispan.server.resp;

import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.context.Flag;
import org.infinispan.server.core.logging.Log;
import org.infinispan.server.resp.operation.SetOperation;
import org.infinispan.server.resp.response.SetResponse;
import org.infinispan.util.concurrent.AggregateCompletionStage;
import org.infinispan.util.concurrent.CompletionStages;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.CharsetUtil;

public class Resp3Handler extends Resp3AuthHandler {
   private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass(), Log.class);
   static final byte[] OK = "+OK\r\n".getBytes(StandardCharsets.US_ASCII);

   protected AdvancedCache<byte[], byte[]> ignoreLoaderCache;

   Resp3Handler(RespServer respServer) {
      super(respServer);
   }

   @Override
   protected void setCache(AdvancedCache<byte[], byte[]> cache) {
      super.setCache(cache);
      ignoreLoaderCache = cache.withFlags(Flag.SKIP_CACHE_LOAD);
   }

   protected static final BiConsumer<byte[], ByteBufPool> GET_BICONSUMER = (innerValueBytes, alloc) -> {
      if (innerValueBytes != null) {
         bytesToResult(innerValueBytes, alloc);
      } else {
         stringToByteBuf("$-1\r\n", alloc);
      }
   };

   protected static final BiConsumer<Object, ByteBufPool> OK_BICONSUMER = (ignore, alloc) ->
         alloc.acquire(OK.length).writeBytes(OK);

   protected static final BiConsumer<SetResponse, ByteBufPool> SET_BICONSUMER = (res, alloc) -> {
      // The set operation has three return options, with a precedence:
      //
      // 1. Previous value or `nil`: when `GET` flag present;
      // 2. `OK`: when set operation succeeded
      // 3. `nil`: when set operation failed, e.g., tried using XX or NX.
      if (res.isReturnValue()) {
         GET_BICONSUMER.accept(res.value(), alloc);
         return;
      }

      if (res.isSuccess()) {
         OK_BICONSUMER.accept(res, alloc);
         return;
      }

      GET_BICONSUMER.accept(null, alloc);
   };

   protected static final BiConsumer<Long, ByteBufPool> LONG_BICONSUMER = Resp3Handler::handleLongResult;

   protected static final BiConsumer<byte[], ByteBufPool> DELETE_BICONSUMER = (prev, alloc) ->
         stringToByteBuf(":" + (prev == null ? "0" : "1") + "\r\n", alloc);

   @Override
   protected CompletionStage<RespRequestHandler> actualHandleRequest(ChannelHandlerContext ctx, String type, List<byte[]> arguments) {
      switch (type) {
         case "PING":
            if (arguments.size() == 0) {
               stringToByteBuf("$4\r\nPONG\r\n", allocatorToUse);
               break;
            }
            // falls-through
         case "ECHO":
            byte[] argument = arguments.get(0);
            ByteBuf bufferToWrite = stringToByteBufWithExtra("$" + argument.length + "\r\n", allocatorToUse, argument.length + 2);
            bufferToWrite.writeBytes(argument);
            bufferToWrite.writeByte('\r').writeByte('\n');
            break;
         case "SET":
            if (arguments.size() != 2) {
               return stageToReturn(SetOperation.performOperation(cache, arguments), ctx, SET_BICONSUMER);
            }
            return stageToReturn(ignoreLoaderCache.putAsync(arguments.get(0), arguments.get(1)), ctx, OK_BICONSUMER);
         case "GET":
            byte[] keyBytes = arguments.get(0);

            return stageToReturn(cache.getAsync(keyBytes), ctx, GET_BICONSUMER);
         case "DEL":
            return performDelete(ctx, cache, arguments);
         case "MGET":
            return performMget(ctx, cache, arguments);
         case "MSET":
            return performMset(ctx, cache, arguments);
         case "INCR":
            return stageToReturn(counterIncOrDec(cache, arguments.get(0), true), ctx, LONG_BICONSUMER);
         case "DECR":
            return stageToReturn(counterIncOrDec(cache, arguments.get(0), false), ctx, LONG_BICONSUMER);
         case "CONFIG":
            String getOrSet = new String(arguments.get(0), StandardCharsets.UTF_8);
            String name = new String(arguments.get(1), StandardCharsets.UTF_8);

            if ("GET".equalsIgnoreCase(getOrSet)) {
               if ("appendonly".equalsIgnoreCase(name)) {
                  stringToByteBuf("*2\r\n+" + name + "\r\n+no\r\n", allocatorToUse);
               } else if (name.indexOf('*') != -1 || name.indexOf('?') != -1) {
                  stringToByteBuf("-ERR CONFIG blob pattern matching not implemented\r\n", allocatorToUse);
               } else {
                  stringToByteBuf("*2\r\n+" + name + "\r\n+\r\n", allocatorToUse);
               }
            } else if ("SET".equalsIgnoreCase(getOrSet)) {
               OK_BICONSUMER.accept(null, allocatorToUse);
            } else {
               stringToByteBuf("-ERR CONFIG " + getOrSet + " not implemented\r\n", allocatorToUse);
            }
            break;
         case "INFO":
            stringToByteBuf("-ERR not implemented yet\r\n", allocatorToUse);
            break;
         case "PUBLISH":
            // TODO: should we return the # of subscribers on this node?
            // We use expiration to remove the event values eventually while preventing them during high periods of
            // updates
            return stageToReturn(ignoreLoaderCache.putAsync(SubscriberHandler.keyToChannel(arguments.get(0)), arguments.get(1), 3, TimeUnit.SECONDS), ctx, (ignore, alloc) -> {
               stringToByteBuf(":0\r\n", alloc);
            });
         case "SUBSCRIBE":
            SubscriberHandler subscriberHandler = new SubscriberHandler(respServer, this);
            return subscriberHandler.handleRequest(ctx, type, arguments);
         case "SELECT":
            stringToByteBuf("-ERR Select not supported in cluster mode\r\n", allocatorToUse);
            break;
         case "READWRITE":
         case "READONLY":
            // We are always in read write allowing read from backups
            OK_BICONSUMER.accept(null, allocatorToUse);
            break;
         case "RESET":
            stringToByteBuf("+RESET\r\n", allocatorToUse);
            if (respServer.getConfiguration().authentication().enabled()) {
               return CompletableFuture.completedFuture(new Resp3AuthHandler(respServer));
            }
            break;
         case "COMMAND":
            if (!arguments.isEmpty()) {
               stringToByteBuf("-ERR COMMAND does not currently support arguments\r\n", allocatorToUse);
               break;
            }
            StringBuilder commandBuilder = new StringBuilder();
            commandBuilder.append("*21\r\n");
            addCommand(commandBuilder, "HELLO", -1, 0, 0, 0);
            addCommand(commandBuilder, "AUTH", -2, 0, 0, 0);
            addCommand(commandBuilder, "PING", -2, 0, 0, 0);
            addCommand(commandBuilder, "ECHO", 2, 0, 0, 0);
            addCommand(commandBuilder, "GET", 2, 1, 1, 1);
            addCommand(commandBuilder, "SET", -3, 1, 1, 1);
            addCommand(commandBuilder, "DEL", -2, 1, -1, 1);
            addCommand(commandBuilder, "CONFIG", -2, 0, 0, 0);
            addCommand(commandBuilder, "MGET", -2, 1, -1, 1);
            addCommand(commandBuilder, "MSET", -3, 1, 1, 2);
            addCommand(commandBuilder, "INCR", 2, 1, 1, 1);
            addCommand(commandBuilder, "DECR", 2, 1, 1, 1);
            addCommand(commandBuilder, "INFO", -1, 0, 0, 0);
            addCommand(commandBuilder, "PUBLISH", 3, 0, 0, 0);
            addCommand(commandBuilder, "SUBSCRIBE", -2, 0, 0, 0);
            addCommand(commandBuilder, "SELECT", -1, 0, 0, 0);
            addCommand(commandBuilder, "READWRITE", 1, 0, 0, 0);
            addCommand(commandBuilder, "READONLY", 1, 0, 0, 0);
            addCommand(commandBuilder, "RESET", 1, 0, 0, 0);
            addCommand(commandBuilder, "QUIT", 1, 0, 0, 0);
            addCommand(commandBuilder, "COMMAND", -1, 0, 0, 0);
            stringToByteBuf(commandBuilder.toString(), allocatorToUse);
            break;
         default:
            return super.actualHandleRequest(ctx, type, arguments);
      }
      return myStage;
   }

   private static void addCommand(StringBuilder builder, String name, int arity, int firstKeyPos, int lastKeyPos, int steps) {
      builder.append("*6\r\n");
      // Name
      builder.append("$").append(ByteBufUtil.utf8Bytes(name)).append("\r\n").append(name).append("\r\n");
      // Arity
      builder.append(":").append(arity).append("\r\n");
      // Flags
      builder.append("*0\r\n");
      // First key
      builder.append(":").append(firstKeyPos).append("\r\n");
      // Second key
      builder.append(":").append(lastKeyPos).append("\r\n");
      // Step
      builder.append(":").append(steps).append("\r\n");

   }

   protected static void handleLongResult(Long result, ByteBufPool alloc) {
      // TODO: this can be optimized to avoid the String allocation
      stringToByteBuf(":" + result + "\r\n", alloc);
   }

   protected static void handleThrowable(ByteBufPool alloc, Throwable t) {
      stringToByteBuf("-ERR " + t.getMessage() + "\r\n", alloc);
   }

   private static CompletionStage<Long> counterIncOrDec(Cache<byte[], byte[]> cache, byte[] key, boolean increment) {
      return cache.getAsync(key)
            .thenCompose(currentValueBytes -> {
               if (currentValueBytes != null) {
                  // Numbers are always ASCII
                  String prevValue = new String(currentValueBytes, CharsetUtil.US_ASCII);
                  long prevIntValue;
                  try {
                     prevIntValue = Long.parseLong(prevValue) + (increment ? 1 : -1);
                  } catch (NumberFormatException e) {
                     throw new CacheException("value is not an integer or out of range");
                  }
                  String newValueString = String.valueOf(prevIntValue);
                  byte[] newValueBytes = newValueString.getBytes(CharsetUtil.US_ASCII);
                  return cache.replaceAsync(key, currentValueBytes, newValueBytes)
                        .thenCompose(replaced -> {
                           if (replaced) {
                              return CompletableFuture.completedFuture(prevIntValue);
                           }
                           return counterIncOrDec(cache, key, increment);
                        });
               }
               long longValue = increment ? 1 : -1;
               byte[] valueToPut = String.valueOf(longValue).getBytes(CharsetUtil.US_ASCII);
               return cache.putIfAbsentAsync(key, valueToPut)
                     .thenCompose(prev -> {
                        if (prev != null) {
                           return counterIncOrDec(cache, key, increment);
                        }
                        return CompletableFuture.completedFuture(longValue);
                     });
            });
   }


   private CompletionStage<RespRequestHandler> performDelete(ChannelHandlerContext ctx, Cache<byte[], byte[]> cache, List<byte[]> arguments) {
      int keysToRemove = arguments.size();
      if (keysToRemove == 1) {
         byte[] keyBytes = arguments.get(0);
         return stageToReturn(cache.removeAsync(keyBytes), ctx, DELETE_BICONSUMER);
      } else if (keysToRemove == 0) {
         // TODO: is this an error?
         stringToByteBuf(":0\r\n", allocatorToUse);
         return myStage;
      } else {
         AtomicInteger removes = new AtomicInteger();
         AggregateCompletionStage<AtomicInteger> deleteStages = CompletionStages.aggregateCompletionStage(removes);
         for (byte[] keyBytesLoop : arguments) {
            deleteStages.dependsOn(cache.removeAsync(keyBytesLoop)
                  .thenAccept(prev -> {
                     if (prev != null) {
                        removes.incrementAndGet();
                     }
                  }));
         }
         return stageToReturn(deleteStages.freeze(), ctx, (removals, alloc) -> {
            stringToByteBuf(":" + removals.get() + "\r\n", alloc);
         });
      }
   }

   private CompletionStage<RespRequestHandler> performMget(ChannelHandlerContext ctx, Cache<byte[], byte[]> cache, List<byte[]> arguments) {
      int keysToRetrieve = arguments.size();
      if (keysToRetrieve == 0) {
         stringToByteBuf("*0\r\n", allocatorToUse);
         return myStage;
      }
      List<byte[]> results = Collections.synchronizedList(Arrays.asList(
            new byte[keysToRetrieve][]));
      AtomicInteger resultBytesSize = new AtomicInteger();
      AggregateCompletionStage<Void> getStage = CompletionStages.aggregateCompletionStage();
      for (int i = 0; i < keysToRetrieve; ++i) {
         int innerCount = i;
         byte[] keyBytes = arguments.get(i);
         getStage.dependsOn(cache.getAsync(keyBytes)
               .whenComplete((returnValue, t) -> {
                  if (returnValue != null) {
                     results.set(innerCount, returnValue);
                     int length = returnValue.length;
                     if (length > 0) {
                        // $ + digit length (log10 + 1) + /r/n + byte length
                        resultBytesSize.addAndGet(1 + (int) Math.log10(length) + 1 + 2 + returnValue.length);
                     } else {
                        // $0 + /r/n
                        resultBytesSize.addAndGet(2 + 2);
                     }
                  } else {
                     // $-1
                     resultBytesSize.addAndGet(3);
                  }
                  // /r/n
                  resultBytesSize.addAndGet(2);
               }));
      }
      return stageToReturn(getStage.freeze(), ctx, (ignore, alloc) -> {
         int elements = results.size();
         // * + digit length (log10 + 1) + \r\n + accumulated bytes
         int byteAmount = 1 + (int) Math.log10(elements) + 1 + 2 + resultBytesSize.get();
         ByteBuf byteBuf = alloc.apply(byteAmount);
         byteBuf.writeCharSequence("*" + results.size(), CharsetUtil.US_ASCII);
         byteBuf.writeByte('\r');
         byteBuf.writeByte('\n');
         for (byte[] value : results) {
            if (value == null) {
               byteBuf.writeCharSequence("$-1", CharsetUtil.US_ASCII);
            } else {
               byteBuf.writeCharSequence("$" + value.length, CharsetUtil.US_ASCII);
               byteBuf.writeByte('\r');
               byteBuf.writeByte('\n');
               byteBuf.writeBytes(value);
            }
            byteBuf.writeByte('\r');
            byteBuf.writeByte('\n');
         }
         assert byteBuf.writerIndex() == byteAmount;
      });
   }

   private CompletionStage<RespRequestHandler> performMset(ChannelHandlerContext ctx, Cache<byte[], byte[]> cache, List<byte[]> arguments) {
      int keyValuePairCount = arguments.size();
      if ((keyValuePairCount & 1) == 1) {
         log.tracef("Received: %s count for keys and values combined, should be even for MSET", keyValuePairCount);
         stringToByteBuf("-ERR Missing a value for a key" + "\r\n", allocatorToUse);
         return myStage;
      }
      AggregateCompletionStage<Void> setStage = CompletionStages.aggregateCompletionStage();
      for (int i = 0; i < keyValuePairCount; i += 2) {
         byte[] keyBytes = arguments.get(i);
         byte[] valueBytes = arguments.get(i + 1);
         setStage.dependsOn(cache.putAsync(keyBytes, valueBytes));
      }
      return stageToReturn(setStage.freeze(), ctx, OK_BICONSUMER);
   }
}
