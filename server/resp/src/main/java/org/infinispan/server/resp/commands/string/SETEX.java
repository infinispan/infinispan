package org.infinispan.server.resp.commands.string;

import static org.infinispan.server.resp.operation.RespExpiration.EX_BYTES;

import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespRequestHandler;

import io.netty.channel.ChannelHandlerContext;

/**
 * `<code>SETEX key seconds value</code>` command.
 * <p>
 * As of Redis 2.6.2, this command is deprecated. Applications should utilize `<code>SET key value EX seconds</code>`.
 * </p>
 *
 * @since 15.0
 * @author José Bolina
 * @see <a href="https://redis.io/commands/setex/">Redis documentation</a>.
 */
public class SETEX extends SET {

   public SETEX() {
      super(4, 1, 1, 1);
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler, ChannelHandlerContext ctx, List<byte[]> arguments) {
      byte[] key = arguments.get(0);
      byte[] seconds = arguments.get(1);
      byte[] value = arguments.get(2);
      return super.perform(handler, ctx, List.of(key, value, EX_BYTES, seconds));
   }
}
