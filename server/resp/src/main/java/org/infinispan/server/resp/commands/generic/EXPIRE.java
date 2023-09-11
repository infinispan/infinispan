package org.infinispan.server.resp.commands.generic;

import static org.infinispan.server.resp.Util.fromUnixTime;
import static org.infinispan.server.resp.Util.toUnixTime;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import org.infinispan.server.resp.Consumers;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.ArgumentUtils;
import org.infinispan.server.resp.commands.Resp3Command;

import io.netty.channel.ChannelHandlerContext;

/**
 * EXPIRE Resp Command
 *
 * @link <a href="https://redis.io/commands/expire/">EXPIRE</a>
 * @since 15.0
 */
public class EXPIRE extends RespCommand implements Resp3Command {

   public static final CompletableFuture<Long> NOT_APPLIED = CompletableFuture.completedFuture(0L);
   public static final CompletableFuture<Long> APPLIED = CompletableFuture.completedFuture(1L);

   enum Mode {
      NONE, NX, XX, GT, LT
   }

   private final boolean unixTime;

   public EXPIRE() {
      this(false);
   }

   protected EXPIRE(boolean at) {
      super(-3, 1, 1, 1);
      this.unixTime = at;
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler,
                                                      ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {
      byte[] key = arguments.get(0);
      long expiration = ArgumentUtils.toLong(arguments.get(1));
      Mode mode = Mode.NONE;
      if (arguments.size() == 3) {
         // Handle mode
         mode = Mode.valueOf(new String(arguments.get(2), StandardCharsets.US_ASCII).toUpperCase());
      }
      return handler.stageToReturn(expire(handler, key, expiration, mode, unixTime), ctx, Consumers.LONG_BICONSUMER);

   }

   private static CompletionStage<Long> expire(Resp3Handler handler, byte[] key, long expiration, Mode mode, boolean unixTime) {
      return handler.cache().getCacheEntryAsync(key).thenCompose(e -> {
         if (e == null) {
            return NOT_APPLIED;
         } else {
            long ttl = e.getLifespan();
            if (unixTime) {
               ttl = toUnixTime(ttl, handler.respServer().getTimeService());
            } else if (ttl >= 0) {
               ttl = TimeUnit.MILLISECONDS.toSeconds(e.getLifespan());
            }
            switch (mode) {
               case NX:
                  if (ttl >= 0) {
                     return NOT_APPLIED;
                  }
                  break;
               case XX:
                  if (ttl < 0) {
                     return NOT_APPLIED;
                  }
                  break;
               case GT:
                  if (expiration < ttl) {
                     return NOT_APPLIED;
                  }
                  break;
               case LT:
                  if (expiration > ttl) {
                     return NOT_APPLIED;
                  }
                  break;
            }
            CompletableFuture<Boolean> replace;
            if (unixTime) {
               replace = handler.cache().replaceAsync(e.getKey(), e.getValue(), e.getValue(), fromUnixTime(expiration, handler.respServer().getTimeService()), TimeUnit.MILLISECONDS);
            } else {
               replace = handler.cache().replaceAsync(e.getKey(), e.getValue(), e.getValue(), expiration, TimeUnit.SECONDS);
            }
            return replace.thenCompose(b -> {
               if (b) {
                  return APPLIED;
               } else {
                  return expire(handler, key, expiration, mode, unixTime);
               }
            });
         }
      });
   }
}
