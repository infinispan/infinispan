package org.infinispan.server.resp.commands.list;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.server.resp.AclCategory;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.serialization.ResponseWriter;

import io.netty.channel.ChannelHandlerContext;

/**
 * LMOVEM
 *
 * @see <a href="https://redis.io/commands/lmovem/">LMOVEM</a>
 * @since 16.3
 */
public class LMOVEM extends AbstractLMOVEM implements Resp3Command {
   public LMOVEM() {
      super(-5, AclCategory.WRITE.mask() | AclCategory.LIST.mask() | AclCategory.SLOW.mask());
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler, ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {
      LmovemConfig config = parseArguments(handler, arguments, false);
      if (config == null) return handler.myStage();

      CompletionStage<Collection<byte[]>> cs = executeLmovem("LMOVEM", handler.getListMultimap(), config);
      return handler.stageToReturn(cs, ctx, ResponseWriter.ARRAY_BULK_STRING_OR_NIL);
   }
}
