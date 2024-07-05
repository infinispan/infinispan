package org.infinispan.server.resp.commands.string;

import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.server.resp.Consumers;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.operation.LCSOperation;

import io.netty.channel.ChannelHandlerContext;

/**
 * `<code>LCS key1 key2 [LEN] [IDX] [MINMATCHLEN min-match-len] [WITHMATCHLEN]</code>` command.
 *
 * @since 15.1
 * @see <a href="https://redis.io/commands/lcs/">Redis Documentation</a>
 * @see STRALGO
 */
public class LCS extends RespCommand implements Resp3Command {

   public LCS() {
      super(-3, 1, 2, 1);
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler, ChannelHandlerContext ctx, List<byte[]> arguments) {
      return handler.stageToReturn(LCSOperation.performOperation(handler.cache(), arguments, true), ctx, Consumers.LCS_BICONSUMER);
   }
}
