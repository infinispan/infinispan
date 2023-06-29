package org.infinispan.server.resp.commands.sortedset.internal;

import io.netty.channel.ChannelHandlerContext;
import org.infinispan.multimap.impl.EmbeddedMultimapSortedSetCache;
import org.infinispan.server.resp.Consumers;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespErrorUtil;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.ArgumentUtils;
import org.infinispan.server.resp.commands.Resp3Command;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionStage;

/**
 * Common implementation for ZPOP commands
 */
public abstract class POP extends RespCommand implements Resp3Command {
   private final boolean min;
   public POP(boolean min) {
      super(-2, 1, 1, 1);
      this.min = min;
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler,
                                                      ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {

      byte[] name = arguments.get(0);
      EmbeddedMultimapSortedSetCache<byte[], byte[]> sortedSetCache = handler.getSortedSeMultimap();

      long count = 1;
      if (arguments.size() > 1) {
         try {
            count = ArgumentUtils.toLong(arguments.get(1));
            if (count < 0) {
               RespErrorUtil.mustBePositive(handler.allocator());
               return handler.myStage();
            }
         } catch (NumberFormatException e) {
            RespErrorUtil.mustBePositive(handler.allocator());
            return handler.myStage();
         }
      }

      CompletionStage<List<byte[]>> popElements = sortedSetCache.pop(name, min, count).thenApply(r -> {
         List<byte[]> result = new ArrayList<>();
         r.stream().forEach(e -> {
            result.add(e.getValue());
            result.add(Double.toString(e.score()).getBytes(StandardCharsets.US_ASCII));
         });
         return result;
      });

      return handler.stageToReturn(popElements, ctx, Consumers.GET_ARRAY_BICONSUMER);
   }
}
