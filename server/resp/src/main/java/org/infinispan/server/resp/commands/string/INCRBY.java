package org.infinispan.server.resp.commands.string;

import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.server.resp.AclCategory;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.ArgumentUtils;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.serialization.ResponseWriter;

import io.netty.channel.ChannelHandlerContext;

/**
 * INCRBY
 *
 * @see <a href="https://redis.io/commands/incrby/">INCRBY</a>
 * @since 15.0
 */
public class INCRBY extends RespCommand implements Resp3Command {
   public INCRBY() {
      super(3, 1, 1, 1);
   }

   @Override
   public long aclMask() {
      return AclCategory.WRITE | AclCategory.STRING | AclCategory.FAST;
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler,
                                                      ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {
      return handler
            .stageToReturn(CounterIncOrDec.counterIncOrDecBy(handler.cache(), arguments.get(0), ArgumentUtils.toLong(arguments.get(1))),
                  ctx, ResponseWriter.INTEGER);
   }
}
