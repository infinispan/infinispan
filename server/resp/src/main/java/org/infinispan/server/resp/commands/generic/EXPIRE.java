package org.infinispan.server.resp.commands.generic;

import static org.infinispan.server.resp.Util.fromUnixTime;
import static org.infinispan.server.resp.Util.toUnixTime;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.time.TimeService;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.ArgumentUtils;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.serialization.Resp3Response;

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
   private final boolean seconds;

   public EXPIRE() {
      this(false, true);
   }

   protected EXPIRE(boolean at, boolean seconds) {
      super(-3, 1, 1, 1);
      this.unixTime = at;
      this.seconds = seconds;
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler,
                                                      ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {
      byte[] key = arguments.get(0);
      long expiration = ArgumentUtils.toLong(arguments.get(1));
      if (seconds) {
         expiration = TimeUnit.SECONDS.toMillis(expiration);
      }
      Mode mode = Mode.NONE;
      if (arguments.size() == 3) {
         // Handle mode
         mode = Mode.valueOf(new String(arguments.get(2), StandardCharsets.US_ASCII).toUpperCase());
      }
      return handler.stageToReturn(expire(handler, key, expiration, mode, unixTime), ctx, Resp3Response.INTEGER);

   }

   private static CompletionStage<Long> expire(Resp3Handler handler, byte[] key, long expiration, Mode mode, boolean unixTime) {
      MediaType vmt = handler.cache().getValueDataConversion().getStorageMediaType();
      final AdvancedCache<byte[], Object> acm = handler.typedCache(vmt);
      return acm.getCacheEntryAsync(key).thenCompose(e -> {
         if (e == null) {
            return NOT_APPLIED;
         } else {
            long ttl = e.getLifespan();
            if (unixTime) {
               ttl = toUnixTime(ttl, handler.respServer().getTimeService());
            } else if (ttl >= 0) {
               ttl = e.getLifespan();
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
            if (expiration <= 0 || (unixTime && isInThePast(expiration, handler.respServer().getTimeService()))) {
               replace = acm.removeAsync(e.getKey(), e.getValue());
            } else if (unixTime) {
               replace = acm.replaceAsync(e.getKey(), e.getValue(), e.getValue(), fromUnixTime(expiration, handler.respServer().getTimeService()), TimeUnit.MILLISECONDS);
            } else {
               replace = acm.replaceAsync(e.getKey(), e.getValue(), e.getValue(), expiration, TimeUnit.MILLISECONDS);
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

   private static boolean isInThePast(long expiration, TimeService timeService) {
      return expiration <= timeService.wallClockTime();
   }
}
