package org.infinispan.server.resp.commands.string;

import static org.infinispan.server.resp.operation.SetOperation.NX_BYTES;

import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.server.resp.Consumers;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.operation.SetOperation;
import org.infinispan.server.resp.response.SetResponse;

import io.netty.channel.ChannelHandlerContext;

/**
 * `<code>SETNX key value</code>` command.
 * <p>
 * This command is deprecated. The alternative is `<code>SET key value NX</code>`.
 * </p>
 *
 * @since 15.0
 * @author José Bolina
 * @see SET
 * @see <a href="https://redis.io/commands/setnx/">Redis documentation</a>.
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
      CompletionStage<SetResponse> cs = SetOperation.performOperation(handler.cache(), List.of(key, value, NX_BYTES), handler.respServer().getTimeService(), getName());
      return handler.stageToReturn(cs, ctx, (res, alloc) -> Consumers.BOOLEAN_BICONSUMER.accept(res.isSuccess(), alloc));
   }
}
