package org.infinispan.server.resp.commands.generic;

import static org.infinispan.server.resp.RespUtil.toUnixTime;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.time.TimeService;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.serialization.ResponseWriter;

import io.netty.channel.ChannelHandlerContext;

/**
 * TTL
 *
 * @see <a href="https://redis.io/commands/ttl/">ttl</a>
 * @since 15.0
 */
public class TTL extends RespCommand implements Resp3Command {
   private final EnumSet<ExpirationOption> options;

   public TTL() {
      this(ExpirationOption.REMAINING, ExpirationOption.SECONDS);
   }

   protected TTL(ExpirationOption ... options) {
      super(2, 1, 1, 1);
      this.options = EnumSet.copyOf(Arrays.asList(options));
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler,
                                                      ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {
      byte[] keyBytes = arguments.get(0);
      MediaType vmt = handler.cache().getValueDataConversion().getStorageMediaType();
      return handler.stageToReturn(handler.cache().withMediaType(MediaType.APPLICATION_OCTET_STREAM, vmt).getCacheEntryAsync(keyBytes).thenApply(e -> {
         if (e == null) {
            return -2L;
         } else {
            if (e.getLifespan() < 0) return -1;

            TimeService timeService = handler.respServer().getTimeService();
            long ttl = options.contains(ExpirationOption.REMAINING)
                  ? e.getLifespan() - (timeService.wallClockTime() - e.getCreated())
                  : e.getLifespan();

            if (options.contains(ExpirationOption.UNIX_TIME))
               ttl = toUnixTime(ttl, timeService);

            if (options.contains(ExpirationOption.SECONDS))
               ttl = ttl / 1_000;

            return ttl;
         }
      }), ctx, ResponseWriter.INTEGER);
   }

   protected enum ExpirationOption {
      REMAINING,
      UNIX_TIME,
      SECONDS;
   }
}
