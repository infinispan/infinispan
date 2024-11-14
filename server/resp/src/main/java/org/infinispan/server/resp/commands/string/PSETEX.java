package org.infinispan.server.resp.commands.string;

import static org.infinispan.server.resp.operation.RespExpiration.PX_BYTES;

import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.server.resp.AclCategory;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespRequestHandler;

import io.netty.channel.ChannelHandlerContext;

/**
 * PSETEX
 * <p>
 * This command is deprecated. The alternative is `<code>SET key value PX milliseconds</code>`.
 * </p>
 *
 * @author José Bolina
 * @see <a href="https://redis.io/commands/psetex/">PSETEX</a>.
 * @see SET
 * @since 15.0
 */
public class PSETEX extends SET {

   public PSETEX() {
      super(4, 1, 1, 1);
   }

   @Override
   public long aclMask() {
      return AclCategory.WRITE | AclCategory.STRING | AclCategory.SLOW;
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler, ChannelHandlerContext ctx, List<byte[]> arguments) {
      byte[] key = arguments.get(0);
      byte[] milliseconds = arguments.get(1);
      byte[] value = arguments.get(2);
      return super.perform(handler, ctx, List.of(key, value, PX_BYTES, milliseconds));
   }
}
