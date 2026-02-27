package org.infinispan.server.resp.commands.string;

import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.commons.time.TimeService;
import org.infinispan.server.resp.AclCategory;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.operation.SetOperation;
import org.infinispan.server.resp.response.SetResponse;
import org.infinispan.server.resp.serialization.ResponseWriter;

import io.netty.channel.ChannelHandlerContext;

/**
 * SET
 *
 * @see <a href="https://redis.io/commands/set/">SET</a>
 * @since 14.0
 */
public class SET extends RespCommand implements Resp3Command {
   public SET() {
      this(-3, 1, 1, 1, AclCategory.WRITE.mask() | AclCategory.STRING.mask() | AclCategory.SLOW.mask());
   }

   protected SET(int arity, int firstKeyPos, int lastKeyPos, int steps, long aclMask) {
      super(arity, firstKeyPos, lastKeyPos, steps, aclMask);
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler,
                                                      ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {
      if (arguments.size() != 2) {
         TimeService ts = handler.respServer().getTimeService();
         return handler
               .stageToReturn(SetOperation.performOperation(handler.cache(), arguments, ts, getName()), ctx, SetResponse.SERIALIZER);
      }
      return handler.stageToReturn(
            handler.cache().putAsync(arguments.get(0), arguments.get(1)),
            ctx, ResponseWriter.OK);
   }
}
