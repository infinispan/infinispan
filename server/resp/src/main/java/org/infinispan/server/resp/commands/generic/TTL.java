package org.infinispan.server.resp.commands.generic;

import static org.infinispan.server.resp.Util.toUnixTime;

import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.serialization.Resp3Response;

import io.netty.channel.ChannelHandlerContext;

/**
 * TTL Resp Command
 * <a href="https://redis.io/commands/ttl/">ttl</a>
 *
 * @since 15.0
 */
public class TTL extends RespCommand implements Resp3Command {
   private final boolean unixTime;
   private final boolean milliseconds;

   public TTL() {
      this(false, false);
   }

   protected TTL(boolean unixTime, boolean milliseconds) {
      super(2, 1, 1, 1);
      this.unixTime = unixTime;
      this.milliseconds = milliseconds;
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
            long ttl = e.getLifespan();
            if (unixTime) {
               ttl = toUnixTime(ttl, handler.respServer().getTimeService());
            }
            if (milliseconds) {
               return ttl;
            } else {
               return ttl < 0 ? ttl : ttl / 1000;
            }
         }
      }), ctx, Resp3Response.INTEGER);
   }
}
