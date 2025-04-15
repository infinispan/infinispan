package org.infinispan.server.resp.commands.search;

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
 * FT._LIST
 *
 * @see <a href="https://redis.io/docs/latest/commands/ft._list/">FT._LIST</a>
 * @since 16.0
 */
public class FT_LIST extends RespCommand implements Resp3Command {
   public static final String NAME = "FT._LIST";

   public FT_LIST() {
      super(NAME, -1, 0, 0, 0, AclCategory.ADMIN.mask() | AclCategory.SEARCH.mask() | AclCategory.SLOW.mask());
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler,
                                                      ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {
      ResponseWriter writer = handler.writer();
      writer.emptySet();
      return handler.myStage();
   }
}
