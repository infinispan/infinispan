package org.infinispan.server.resp.commands.string;

import static org.infinispan.server.resp.operation.RespExpiration.EX_BYTES;

import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.server.resp.AclCategory;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespRequestHandler;

import io.netty.channel.ChannelHandlerContext;

/**
 * SETEX
 * <p>
 * As of Redis 2.6.2, this command is deprecated. Applications should utilize `<code>SET key value EX seconds</code>`.
 * </p>
 *
 * @author Jos&eacute; Bolina
 * @see <a href="https://redis.io/commands/setex/">SETEX</a>.
 * @since 15.0
 */
public class SETEX extends SET {

   public SETEX() {
      super(4, 1, 1, 1, AclCategory.WRITE.mask() | AclCategory.STRING.mask() | AclCategory.SLOW.mask());
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler, ChannelHandlerContext ctx, List<byte[]> arguments) {
      byte[] key = arguments.get(0);
      byte[] seconds = arguments.get(1);
      byte[] value = arguments.get(2);
      return super.perform(handler, ctx, List.of(key, value, EX_BYTES, seconds));
   }
}
