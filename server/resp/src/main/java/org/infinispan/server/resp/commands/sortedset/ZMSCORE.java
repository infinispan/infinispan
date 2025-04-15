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
 * ZMSCORE
 *
 * @see <a href="https://redis.io/commands/zmscore/">ZMSCORE</a>
 * @since 15.0
 */
public class ZMSCORE extends RespCommand implements Resp3Command {
   public ZMSCORE() {
      super(-3, 1, 1, 1, AclCategory.READ.mask() | AclCategory.SORTEDSET.mask() | AclCategory.FAST.mask());
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler,
                                                      ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {

      byte[] name = arguments.get(0);
      EmbeddedMultimapSortedSetCache<byte[], byte[]> sortedSetCache = handler.getSortedSeMultimap();
      return handler.stageToReturn(sortedSetCache.scores(name, arguments.subList(1, arguments.size())),
            ctx, ResponseWriter.ARRAY_DOUBLE);
   }

}
