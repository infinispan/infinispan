package org.infinispan.server.resp.commands.list.blocking;

import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.server.resp.AclCategory;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.serialization.ResponseWriter;

import io.netty.channel.ChannelHandlerContext;

/**
 * BLMOVE
 *
 * @see <a href="https://redis.io/commands/blmove/">BLMOVE</a>
 * @since 16.4
 */
public class BLMOVE extends BLMOVEM {
   public BLMOVE() {
      super(6,
            AclCategory.WRITE.mask() | AclCategory.LIST.mask() | AclCategory.SLOW.mask() | AclCategory.BLOCKING.mask());
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler, ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {
      LmovemConfig config = parseArguments(handler, arguments, true);
      if (config == null) return handler.myStage();

      CompletionStage<byte[]> cs = moveOrBlock(handler, ctx, config)
            .thenApply(elements -> elements == null || elements.isEmpty() ? null : elements.iterator().next());
      return handler.stageToReturn(cs, ctx, ResponseWriter.BULK_STRING_BYTES);
   }
}
