package org.infinispan.server.resp.commands.sortedset;

import io.netty.channel.ChannelHandlerContext;
import org.infinispan.server.resp.Consumers;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespErrorUtil;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * When all the elements in a sorted set are inserted with the same score, in order to force lexicographical
 * ordering, this command returns the number of elements in the sorted set at key with a value between min and max.
 *
 * The min and max arguments have the same meaning as described for {@link ZRANGEBYLEX}.
 *
 * @since 15.0
 * @see <a href="https://redis.io/commands/zlexcount">Redis Documentation</a>
 */
public class ZLEXCOUNT extends RespCommand implements Resp3Command {

   public ZLEXCOUNT() {
      super(4, 1, 1, 1);
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler,
                                                      ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {
      byte[] name = arguments.get(0);
      ZSetCommonUtils.Lex lexFrom = ZSetCommonUtils.parseLex(arguments.get(1));
      ZSetCommonUtils.Lex lexTo = ZSetCommonUtils.parseLex(arguments.get(2));

      if (lexFrom == null || lexTo == null) {
         RespErrorUtil.minOrMaxNotAValidStringRange(handler.allocator());
         return handler.myStage();
      }
      if (lexFrom.unboundedMax || lexTo.unboundedMin) {
         // minLex + or maxLex -,return 0 without performing any call
         return handler.stageToReturn(CompletableFuture.completedFuture(0L), ctx, Consumers.LONG_BICONSUMER);
      }

      return handler.stageToReturn(handler.getSortedSeMultimap()
            .count(name, lexFrom.value, lexFrom.include, lexTo.value, lexTo.include), ctx, Consumers.LONG_BICONSUMER);
   }
}
