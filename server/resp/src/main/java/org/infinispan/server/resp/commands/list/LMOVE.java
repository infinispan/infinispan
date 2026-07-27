package org.infinispan.server.resp.commands.list;

import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.server.resp.AclCategory;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.serialization.ResponseWriter;

import io.netty.channel.ChannelHandlerContext;

/**
 * LMOVE
 *
 * @see <a href="https://redis.io/commands/lmove/">LMOVE</a>
 * @since 15.0
 */
public class LMOVE extends AbstractLMOVEM implements Resp3Command {
   public LMOVE(int arity, long aclMask) {
      super(arity, aclMask);
   }

   public LMOVE() {
      this(5, AclCategory.WRITE.mask() | AclCategory.LIST.mask() | AclCategory.SLOW.mask());
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler, ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {
      return lmoveAndReturn(handler, ctx, arguments, false);
   }

   protected CompletionStage<RespRequestHandler> lmoveAndReturn(Resp3Handler handler,
                                                                ChannelHandlerContext ctx,
                                                                List<byte[]> arguments,
                                                                boolean rightLeft) {
      LmovemConfig config;
      if (rightLeft) {
         // RPOPLPUSH has no direction arguments: always pop from the right, push to the left
         config = new LmovemConfig(arguments.get(0), arguments.get(1), false, true, 1, false, true, 0);
      } else {
         config = parseArguments(handler, arguments, false);
      }
      if (config == null) return handler.myStage();

      CompletionStage<byte[]> cs = executeLmovem(getName(), handler.getListMultimap(), config)
            .thenApply(elements -> elements == null || elements.isEmpty() ? null : elements.iterator().next());
      return handler.stageToReturn(cs, ctx, ResponseWriter.BULK_STRING_BYTES);
   }
}
