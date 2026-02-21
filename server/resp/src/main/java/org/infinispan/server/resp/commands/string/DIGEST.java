package org.infinispan.server.resp.commands.string;

import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.server.resp.AclCategory;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;

import io.netty.channel.ChannelHandlerContext;

/**
 * DIGEST key
 * <p>
 * Returns the XXH3 hash digest of a string value as a hexadecimal string.
 *
 * @see <a href="https://redis.io/commands/digest/">DIGEST</a>
 * @since 16.2
 */
public class DIGEST extends RespCommand implements Resp3Command {

   public DIGEST() {
      super(2, 1, 1, 1, AclCategory.READ.mask() | AclCategory.STRING.mask() | AclCategory.FAST.mask());
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler,
                                                      ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {
      byte[] keyBytes = arguments.get(0);

      return handler.stageToReturn(
            handler.cache().getAsync(keyBytes).thenApply(value -> {
               if (value == null) {
                  return null;
               }
               return XXH3.hashToHex(value);
            }),
            ctx,
            (res, writer) -> writer.string(res)
      );
   }
}
