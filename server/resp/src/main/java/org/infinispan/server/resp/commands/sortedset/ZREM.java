package org.infinispan.server.resp.commands.sortedset;

import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.multimap.impl.EmbeddedMultimapSortedSetCache;
import org.infinispan.server.resp.AclCategory;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.serialization.ResponseWriter;

import io.netty.channel.ChannelHandlerContext;

/**
 * ZREM
 *
 * @see <a href="https://redis.io/commands/zrem/">ZREM</a>
 * @since 15.0
 */
public class ZREM extends RespCommand implements Resp3Command {
   public ZREM() {
      super(-3, 1, 1, 1);
   }

   @Override
   public long aclMask() {
      return AclCategory.WRITE | AclCategory.SORTEDSET | AclCategory.FAST;
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler,
                                                      ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {

      EmbeddedMultimapSortedSetCache<byte[], byte[]> sortedSetCache = handler.getSortedSeMultimap();
      return handler.stageToReturn(sortedSetCache.removeAll(arguments.get(0), arguments.subList(1, arguments.size())),
            ctx, ResponseWriter.INTEGER);
   }

}
