package org.infinispan.server.resp.commands.generic;

import io.netty.channel.ChannelHandlerContext;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;

import java.util.List;
import java.util.concurrent.CompletionStage;

/**
 * Read-only variant of {@link SORT}
 *
 * @since 15.0
 * @see <a href="https://redis.io/commands/sort_ro/">Redis Documentation</a>
 */
public class SORT_RO extends RespCommand implements Resp3Command {
   private final SORT sort;
   public SORT_RO() {
      super(-2, 1, 1, 1);
      sort = new SORT();
      sort.disableStore();
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler,
                                                      ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {

      return sort.perform(handler, ctx, arguments);
   }

}
