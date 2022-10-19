package org.infinispan.server.resp;

import java.lang.invoke.MethodHandles;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.Cache;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.server.core.logging.Log;
import org.infinispan.util.concurrent.AggregateCompletionStage;
import org.infinispan.util.concurrent.CompletionStages;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.CharsetUtil;

public class Resp3Handler extends Resp3AuthHandler {
   private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass(), Log.class);
   private static final ByteBuf OK = RespRequestHandler.stringToByteBuf("+OK\r\n", ByteBufAllocator.DEFAULT);

   Resp3Handler(RespServer respServer) {
      super(respServer);
   }

   // Returns a cached OK status that is retained for multiple uses
   static ByteBuf statusOK() {
      return OK.retain();
   }

   @Override
   public CompletionStage<RespRequestHandler> handleRequest(ChannelHandlerContext ctx, String type, List<byte[]> arguments) {
      switch (type) {
         case "PING":
            if (arguments.size() == 0) {
               ctx.writeAndFlush(RespRequestHandler.stringToByteBuf("$4\r\nPONG\r\n", ctx.alloc()));
               break;
            }
            // falls-through
         case "ECHO":
            byte[] argument = arguments.get(0);
            ByteBuf bufferToWrite = RespRequestHandler.stringToByteBufWithExtra("$" + argument.length + "\r\n", ctx.alloc(), argument.length + 2);
            bufferToWrite.writeBytes(argument);
            bufferToWrite.writeByte('\r').writeByte('\n');
            ctx.writeAndFlush(bufferToWrite);
            break;
         case "SET":
            return performSet(ctx, cache, arguments.get(0), arguments.get(1), -1, type, statusOK());
         case "GET":
            byte[] keyBytes = arguments.get(0);

            return stageToReturn(cache.getAsync(keyBytes), ctx, (innerValueBytes, t) -> {
               if (t != null) {
                  log.trace("Exception encountered while performing GET", t);
                  handleThrowable(ctx, t);
               } else if (innerValueBytes != null) {
                  int length = innerValueBytes.length;
                  ByteBuf buf = RespRequestHandler.stringToByteBufWithExtra("$" + length + "\r\n", ctx.alloc(), length + 2);
                  buf.writeBytes(innerValueBytes);
                  buf.writeByte('\r').writeByte('\n');
                  ctx.writeAndFlush(buf);
               } else {
                  ctx.writeAndFlush(RespRequestHandler.stringToByteBuf("$-1\r\n", ctx.alloc()));
               }
            });
         case "DEL":
            return performDelete(ctx, cache, arguments);
         case "MGET":
            return performMget(ctx, cache, arguments);
         case "MSET":
            return performMset(ctx, cache, arguments);
         case "INCR":
            return stageToReturn(counterIncOrDec(cache, arguments.get(0), true), ctx, (longValue, t) -> {
               if (t != null) {
                  handleThrowable(ctx, t);
               } else {
                  handleLongResult(ctx, longValue);
               }
            });
         case "DECR":
            return stageToReturn(counterIncOrDec(cache, arguments.get(0), false), ctx, (longValue, t) -> {
               if (t != null) {
                  handleThrowable(ctx, t);
               } else {
                  handleLongResult(ctx, longValue);
               }
            });
         case "INFO":
            ctx.writeAndFlush(RespRequestHandler.stringToByteBuf("-ERR not implemented yet\r\n", ctx.alloc()));
            break;
         case "PUBLISH":
            // TODO: should we return the # of subscribers on this node?
            // We use expiration to remove the event values eventually while preventing them during high periods of
            // updates
            return performSet(ctx, cache, SubscriberHandler.keyToChannel(arguments.get(0)),
                  arguments.get(1), 3, type, RespRequestHandler.stringToByteBuf(":0\r\n", ctx.alloc()));
         case "SUBSCRIBE":
            SubscriberHandler subscriberHandler = new SubscriberHandler(respServer, this);
            return subscriberHandler.handleRequest(ctx, type, arguments);
         case "SELECT":
            ctx.writeAndFlush(RespRequestHandler.stringToByteBuf("-ERR Select not supported in cluster mode\r\n", ctx.alloc()));
            break;
         case "READWRITE":
         case "READONLY":
            // We are always in read write allowing read from backups
            ctx.writeAndFlush(statusOK());
            break;
         case "RESET":
            ctx.writeAndFlush(RespRequestHandler.stringToByteBuf("+RESET\r\n", ctx.alloc()));
            if (respServer.getConfiguration().authentication().enabled()) {
               return CompletableFuture.completedFuture(new Resp3AuthHandler(respServer));
            }
            break;
         case "COMMAND":
            if (!arguments.isEmpty()) {
               ctx.writeAndFlush(RespRequestHandler.stringToByteBuf("-ERR COMMAND does not currently support arguments\r\n", ctx.alloc()));
               break;
            }
            StringBuilder commandBuilder = new StringBuilder();
            commandBuilder.append("*20\r\n");
            addCommand(commandBuilder, "HELLO", -1, 0, 0, 0);
            addCommand(commandBuilder, "AUTH", -2, 0, 0, 0);
            addCommand(commandBuilder, "PING", -2, 0, 0, 0);
            addCommand(commandBuilder, "ECHO", 2, 0, 0, 0);
            addCommand(commandBuilder, "GET", 2, 1, 1, 1);
            addCommand(commandBuilder, "SET", -3, 1, 1, 1);
            addCommand(commandBuilder, "DEL", -2, 1, -1, 1);
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
            ctx.writeAndFlush(RespRequestHandler.stringToByteBuf(commandBuilder.toString(), ctx.alloc()));
            break;
         default:
            return super.handleRequest(ctx, type, arguments);
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

   private static void handleLongResult(ChannelHandlerContext ctx, Long result) {
      ctx.writeAndFlush(RespRequestHandler.stringToByteBuf(":" + result + "\r\n", ctx.alloc()));
   }

   static void handleThrowable(ChannelHandlerContext ctx, Throwable t) {
      ctx.writeAndFlush(RespRequestHandler.stringToByteBuf("-ERR" + t.getMessage() + "\r\n", ctx.alloc()));
   }

   private static CompletionStage<Long> counterIncOrDec(Cache<byte[], byte[]> cache, byte[] key, boolean increment) {
      return cache.getAsync(key)
            .thenCompose(currentValueBytes -> {
               if (currentValueBytes != null) {
                  String prevValue = new String(currentValueBytes, CharsetUtil.UTF_8);
                  long prevIntValue;
                  try {
                     prevIntValue = Long.parseLong(prevValue) + (increment ? 1 : -1);
                  } catch (NumberFormatException e) {
                     throw new CacheException("value is not an integer or out of range");
                  }
                  String newValueString = String.valueOf(prevIntValue);
                  byte[] newValueBytes = newValueString.getBytes(CharsetUtil.UTF_8);
                  return cache.replaceAsync(key, currentValueBytes, newValueBytes)
                        .thenCompose(replaced -> {
                           if (replaced) {
                              return CompletableFuture.completedFuture(prevIntValue);
                           }
                           return counterIncOrDec(cache, key, increment);
                        });
               }
               long longValue = increment ? 1 : -1;
               byte[] valueToPut = String.valueOf(longValue).getBytes(CharsetUtil.UTF_8);
               return cache.putIfAbsentAsync(key, valueToPut)
                     .thenCompose(prev -> {
                        if (prev != null) {
                           return counterIncOrDec(cache, key, increment);
                        }
                        return CompletableFuture.completedFuture(longValue);
                     });
            });
   }

   private CompletionStage<RespRequestHandler> performSet(ChannelHandlerContext ctx, Cache<byte[], byte[]> cache, byte[] key, byte[] value,
         long lifespan, String type, ByteBuf messageOnSuccess) {
      return stageToReturn(cache.putAsync(key, value, lifespan, TimeUnit.SECONDS), ctx, (ignore, t) -> {
         if (t != null) {
            log.trace("Exception encountered while performing " + type, t);
            handleThrowable(ctx, t);
         } else {
            ctx.writeAndFlush(messageOnSuccess);
         }
      });
   }

   private CompletionStage<RespRequestHandler> performDelete(ChannelHandlerContext ctx, Cache<byte[], byte[]> cache, List<byte[]> arguments) {
      int keysToRemove = arguments.size();
      if (keysToRemove == 1) {
         byte[] keyBytes = arguments.get(0);
         return stageToReturn(cache.removeAsync(keyBytes), ctx, (prev, t) -> {
            if (t != null) {
               log.trace("Exception encountered while performing DEL", t);
               handleThrowable(ctx, t);
               return;
            }
            ctx.writeAndFlush(RespRequestHandler.stringToByteBuf(":" + (prev == null ? "0" : "1") +
                  "\r\n", ctx.alloc()));
         });
      } else if (keysToRemove == 0) {
         // TODO: is this an error?
         ctx.writeAndFlush(RespRequestHandler.stringToByteBuf(":0\r\n", ctx.alloc()));
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
         return stageToReturn(deleteStages.freeze(), ctx, (removals, t) -> {
            if (t != null) {
               log.trace("Exception encountered while performing multiple DEL", t);
               handleThrowable(ctx, t);
               return;
            }
            ctx.writeAndFlush(RespRequestHandler.stringToByteBuf(":" + removals.get() + "\r\n", ctx.alloc()));
         });
      }
   }

   private CompletionStage<RespRequestHandler> performMget(ChannelHandlerContext ctx, Cache<byte[], byte[]> cache, List<byte[]> arguments) {
      int keysToRetrieve = arguments.size();
      if (keysToRetrieve == 0) {
         ctx.writeAndFlush(RespRequestHandler.stringToByteBuf("*0\r\n", ctx.alloc()));
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
                        // byte length + digit length (log10 + 1) + $
                        resultBytesSize.addAndGet(returnValue.length + (int) Math.log10(length) + 1 + 1);
                     } else {
                        // $0
                        resultBytesSize.addAndGet(2);
                     }
                  } else {
                     // $-1
                     resultBytesSize.addAndGet(3);
                  }
                  // /r/n
                  resultBytesSize.addAndGet(2);
               }));
      }
      return stageToReturn(getStage.freeze(), ctx, (ignore, t) -> {
         if (t != null) {
            log.trace("Exception encountered while performing multiple GET", t);
            handleThrowable(ctx, t);
            return;
         }
         int elements = results.size();
         // * + digit length (log10 + 1) + \r\n
         ByteBuf byteBuf = ctx.alloc().buffer(resultBytesSize.addAndGet(1 + (int) Math.log10(elements)
               + 1 + 2));
         byteBuf.writeCharSequence("*" + results.size(), CharsetUtil.UTF_8);
         byteBuf.writeByte('\r');
         byteBuf.writeByte('\n');
         for (byte[] value : results) {
            if (value == null) {
               byteBuf.writeCharSequence("$-1", CharsetUtil.UTF_8);
            } else {
               byteBuf.writeCharSequence("$" + value.length, CharsetUtil.UTF_8);
               byteBuf.writeByte('\r');
               byteBuf.writeByte('\n');
               byteBuf.writeBytes(value);
            }
            byteBuf.writeByte('\r');
            byteBuf.writeByte('\n');
         }
         ctx.writeAndFlush(byteBuf);
      });
   }

   private CompletionStage<RespRequestHandler> performMset(ChannelHandlerContext ctx, Cache<byte[], byte[]> cache, List<byte[]> arguments) {
      int keyValuePairCount = arguments.size();
      if ((keyValuePairCount & 1) == 1) {
         log.tracef("Received: %s count for keys and values combined, should be even for MSET", keyValuePairCount);
         ctx.writeAndFlush(RespRequestHandler.stringToByteBuf("-ERR Missing a value for a key" + "\r\n", ctx.alloc()));
         return myStage;
      }
      AggregateCompletionStage<Void> setStage = CompletionStages.aggregateCompletionStage();
      for (int i = 0; i < keyValuePairCount; i += 2) {
         byte[] keyBytes = arguments.get(i);
         byte[] valueBytes = arguments.get(i + 1);
         setStage.dependsOn(cache.putAsync(keyBytes, valueBytes));
      }
      return stageToReturn(setStage.freeze(), ctx, (ignore, t) -> {
         if (t != null) {
            log.trace("Exception encountered while performing MSET", t);
            handleThrowable(ctx, t);
         } else {
            ctx.writeAndFlush(statusOK());
         }
      });
   }
}
