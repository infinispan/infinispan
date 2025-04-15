package org.infinispan.server.resp.commands.sortedset;

import static org.infinispan.server.resp.commands.sortedset.ZSetCommonUtils.isWithScoresArg;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.multimap.impl.EmbeddedMultimapSortedSetCache;
import org.infinispan.multimap.impl.SortedSetBucket;
import org.infinispan.server.resp.AclCategory;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.serialization.ResponseWriter;

import io.netty.channel.ChannelHandlerContext;

/**
 * ZRANK
 *
 * @see <a href="https://redis.io/commands/zrank/">ZRANK</a>
 * @since 15.0
 */
public class ZRANK extends RespCommand implements Resp3Command {
   protected boolean isRev;

   public ZRANK() {
      super(-3, 1, 1, 1, AclCategory.READ.mask() | AclCategory.SORTEDSET.mask() | AclCategory.FAST.mask());
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler,
                                                      ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {
      byte[] name = arguments.get(0);
      byte[] member = arguments.get(1);
      boolean withScore = false;
      if (arguments.size() > 2) {
         withScore = isWithScoresArg(arguments.get(2));
         if (!withScore) {
            handler.writer().syntaxError();
            return handler.myStage();
         }
      }

      EmbeddedMultimapSortedSetCache<byte[], byte[]> sortedSet = handler.getSortedSeMultimap();
      if (withScore) {
         return handler.stageToReturn(sortedSet.indexOf(name, member, isRev).thenApply(ZRANK::mapResult), ctx, ResponseWriter.ARRAY_INTEGER);
      }
      return handler.stageToReturn(sortedSet.indexOf(name, member, isRev).thenApply(r -> r == null ? null : r.getValue()), ctx, ResponseWriter.INTEGER);
   }

   private static Collection<Number> mapResult(SortedSetBucket.IndexValue index) {
      if (index == null) {
         return null;
      }
      return List.of(index.getValue(), index.getScore());
   }
}
