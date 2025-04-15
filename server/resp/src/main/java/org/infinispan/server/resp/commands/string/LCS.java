package org.infinispan.server.resp.commands.string;

import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.server.resp.AclCategory;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.operation.LCSOperation;
import org.infinispan.server.resp.response.LCSResponse;

import io.netty.channel.ChannelHandlerContext;

/**
 * LCS
 *
 * @see <a href="https://redis.io/commands/lcs/">LCS</a>
 * @see STRALGO
 * @since 15.1
 */
public class LCS extends RespCommand implements Resp3Command {

   public LCS() {
      super(-3, 1, 2, 1, AclCategory.READ.mask() | AclCategory.STRING.mask() | AclCategory.SLOW.mask());
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler, ChannelHandlerContext ctx, List<byte[]> arguments) {
      return handler.stageToReturn(LCSOperation.performOperation(handler.cache(), arguments, true), ctx, LCSResponse.SERIALIZER);
   }
}
