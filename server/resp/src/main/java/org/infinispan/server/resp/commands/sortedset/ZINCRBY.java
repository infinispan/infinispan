package org.infinispan.server.resp.commands.sortedset;

import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.multimap.impl.EmbeddedMultimapSortedSetCache;
import org.infinispan.multimap.impl.SortedSetAddArgs;
import org.infinispan.server.resp.AclCategory;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.ArgumentUtils;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.serialization.ResponseWriter;

import io.netty.channel.ChannelHandlerContext;

/**
 * ZINCRBY
 *
 * @see <a href="https://redis.io/commands/zincrby/">ZINCRBY</a>
 * @since 15.0
 */
public class ZINCRBY extends RespCommand implements Resp3Command {
   public ZINCRBY() {
      super(4, 1, 1, 1, AclCategory.WRITE.mask() | AclCategory.SORTEDSET.mask() | AclCategory.FAST.mask());
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler,
                                                      ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {
      byte[] name = arguments.get(0);
      double score;
      try {
         score = ArgumentUtils.toDouble(arguments.get(1));
      } catch (Exception ex) {
         handler.writer().valueNotAValidFloat();
         return handler.myStage();
      }
      byte[] value = arguments.get(2);

      EmbeddedMultimapSortedSetCache<byte[], byte[]> sortedSetCache = handler.getSortedSeMultimap();
      return handler.stageToReturn(sortedSetCache.incrementScore(name, score, value, SortedSetAddArgs.create().incr().build()),
            ctx, ResponseWriter.DOUBLE);
   }
}
