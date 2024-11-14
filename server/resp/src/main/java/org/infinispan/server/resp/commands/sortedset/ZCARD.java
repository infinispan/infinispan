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
 * ZCOUNT
 *
 * @see <a href="https://redis.io/commands/zcard/">ZCARD</a>
 * @since 15.0
 */
public class ZCARD extends RespCommand implements Resp3Command {

   public ZCARD() {
      super(2, 1, 1, 1);
   }

   @Override
   public long aclMask() {
      return AclCategory.READ | AclCategory.SORTEDSET | AclCategory.FAST;
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler,
                                                      ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {

      byte[] name = arguments.get(0);
      EmbeddedMultimapSortedSetCache<byte[], ?> sortedSetCache = handler.getSortedSeMultimap();
      return handler.stageToReturn(sortedSetCache.size(name), ctx, ResponseWriter.INTEGER);
   }
}
