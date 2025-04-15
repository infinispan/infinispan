package org.infinispan.server.resp.commands.string;

import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.server.resp.AclCategory;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.serialization.ResponseWriter;

import io.netty.channel.ChannelHandlerContext;

/**
 * DECR
 *
 * @see <a href="https://redis.io/commands/decr/">DECR</a>
 * @since 14.0
 */
public class DECR extends RespCommand implements Resp3Command {
   public DECR() {
      super(2, 1, 1, 1, AclCategory.WRITE.mask() | AclCategory.STRING.mask() | AclCategory.FAST.mask());
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler,
                                                      ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {
      return handler.stageToReturn(CounterIncOrDec.counterIncOrDec(handler.cache(), arguments.get(0), false), ctx, ResponseWriter.INTEGER);
   }
}
