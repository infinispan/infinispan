package org.infinispan.server.resp.commands.string;

import static org.infinispan.server.resp.operation.SetOperation.GET_BYTES;

import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespRequestHandler;

import io.netty.channel.ChannelHandlerContext;

/**
 * `<code>GETSET key value</code>` command.
 * <p>
 * This command is deprecated. The alternative is `<code>SET key value GET</code>`.
 * </p>
 * @since 15.0
 * @author José Bolina
 * @see SET
 * @see <a href="https://redis.io/commands/getset/">Redis documentation</a>.
 */
public class GETSET extends SET {

   public GETSET() {
      super(3, 1, 1, 1);
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler, ChannelHandlerContext ctx, List<byte[]> arguments) {
      byte[] key = arguments.get(0);
      byte[] value = arguments.get(1);
      return super.perform(handler, ctx, List.of(key, value, GET_BYTES));
   }
}
