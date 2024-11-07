package org.infinispan.server.resp.commands.string;

import static org.infinispan.server.resp.operation.SetOperation.NX_BYTES;

import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.operation.SetOperation;
import org.infinispan.server.resp.serialization.ResponseWriter;

import io.netty.channel.ChannelHandlerContext;

/**
 * SETNX
 * <p>
 * This command is deprecated. The alternative is `<code>SET key value NX</code>`.
 * </p>
 *
 * @author José Bolina
 * @see <a href="https://redis.io/commands/setnx/">SETNX</a>.
 * @see SET
 * @since 15.0
 */
public class SETNX extends RespCommand implements Resp3Command {

   public SETNX() {
      super(3, 1, 1, 1);
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler, ChannelHandlerContext ctx, List<byte[]> arguments) {
      byte[] key = arguments.get(0);
      byte[] value = arguments.get(1);

      // Despite the recommended command to replace, the return of SETNX is a boolean instead of an OK.
      CompletionStage<Integer> cs = SetOperation.performOperation(handler.cache(), List.of(key, value, NX_BYTES), handler.respServer().getTimeService(), getName())
            .thenApply(r -> r.isSuccess() ? 1 : 0);
      return handler.stageToReturn(cs, ctx, ResponseWriter.INTEGER);
   }
}
