package org.infinispan.server.resp.commands.set;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletionStage;

import org.infinispan.commons.marshall.WrappedByteArray;
import org.infinispan.server.resp.Consumers;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.util.concurrent.AggregateCompletionStage;
import org.infinispan.util.concurrent.CompletionStages;

import io.netty.channel.ChannelHandlerContext;

/**
 * {@link} https://redis.io/commands/sinterstore/
 *
 * This command is equal to SINTER, but instead of returning the resulting set,
 * it is stored in destination. If destination already exists, it is
 * overwritten.
 *
 * @since 15.0
 */
public class SINTERSTORE extends RespCommand implements Resp3Command {
   static Set<WrappedByteArray> EMPTY_SET = new HashSet<>();

   public SINTERSTORE() {
      super(-3, 1, -1, 1);
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler,
         ChannelHandlerContext ctx,
         List<byte[]> arguments) {

      var destination = arguments.get(0);
      var keys = arguments.subList(1, arguments.size());

      AggregateCompletionStage<Void> acs = CompletionStages.aggregateCompletionStage();
      var sets = SINTER.aggregateSets(handler, keys, acs);
      return handler.stageToReturn(
            acs.freeze().thenCompose(v -> handler.getEmbeddedSetCache().set(destination, SINTER.intersect(sets, 0))),
            ctx,
            Consumers.LONG_BICONSUMER);
   }
}
