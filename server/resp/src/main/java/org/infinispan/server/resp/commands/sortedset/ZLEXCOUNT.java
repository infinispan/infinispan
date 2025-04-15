package org.infinispan.server.resp.commands.sortedset;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.infinispan.server.resp.AclCategory;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.serialization.ResponseWriter;

import io.netty.channel.ChannelHandlerContext;

/**
 * ZLEXCOUNT
 *
 * @see <a href="https://redis.io/commands/zlexcount/">ZLEXCOUNT</a>
 * @since 15.0
 */
public class ZLEXCOUNT extends RespCommand implements Resp3Command {

   public ZLEXCOUNT() {
      super(4, 1, 1, 1, AclCategory.READ.mask() | AclCategory.SORTEDSET.mask() | AclCategory.FAST.mask());
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler,
                                                      ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {
      byte[] name = arguments.get(0);
      ZSetCommonUtils.Lex lexFrom = ZSetCommonUtils.parseLex(arguments.get(1));
      ZSetCommonUtils.Lex lexTo = ZSetCommonUtils.parseLex(arguments.get(2));

      if (lexFrom == null || lexTo == null) {
         handler.writer().minOrMaxNotAValidStringRange();
         return handler.myStage();
      }
      if (lexFrom.unboundedMax || lexTo.unboundedMin) {
         // minLex + or maxLex -,return 0 without performing any call
         return handler.stageToReturn(CompletableFuture.completedFuture(0L), ctx, ResponseWriter.INTEGER);
      }

      return handler.stageToReturn(handler.getSortedSeMultimap()
            .count(name, lexFrom.value, lexFrom.include, lexTo.value, lexTo.include), ctx, ResponseWriter.INTEGER);
   }
}
