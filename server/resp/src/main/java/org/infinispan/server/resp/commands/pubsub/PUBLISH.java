package org.infinispan.server.resp.commands.pubsub;

import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.infinispan.server.resp.AclCategory;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.serialization.ResponseWriter;

import io.netty.channel.ChannelHandlerContext;

/**
 * PUBLISH
 *
 * @see <a href="https://redis.io/commands/publish/">PUBLISH</a>
 * @since 14.0
 */
public class PUBLISH extends RespCommand implements Resp3Command {

   private static final Function<Object, Long> CONVERT = ignore -> 0L;

   public PUBLISH() {
      super(3, 0, 0, 0, AclCategory.PUBSUB.mask() | AclCategory.FAST.mask());
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler,
                                                      ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {
      // TODO: should we return the # of subscribers on this node?
      // We use expiration to remove the event values eventually while preventing them during high periods of
      // updates
      CompletionStage<Long> cs = handler.ignorePreviousValuesCache()
            .putAsync(KeyChannelUtils.keyToChannel(arguments.get(0)), arguments.get(1), 3, TimeUnit.SECONDS)
            .thenApply(CONVERT);
      return handler.stageToReturn(cs, ctx, ResponseWriter.INTEGER);
   }
}
