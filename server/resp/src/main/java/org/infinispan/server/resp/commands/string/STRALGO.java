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
 * @since 15.0
 */
public class STRALGO extends RespCommand implements Resp3Command {
   public STRALGO() {
      super(-5, 1, 2, 1);
   }

   @Override
   public long aclMask() {
      return AclCategory.READ | AclCategory.STRING | AclCategory.SLOW;
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler,
         ChannelHandlerContext ctx,
         List<byte[]> arguments) {
      return handler.stageToReturn(LCSOperation.performOperation(handler.cache(), arguments, false), ctx, LCSResponse.SERIALIZER);
   }


}
