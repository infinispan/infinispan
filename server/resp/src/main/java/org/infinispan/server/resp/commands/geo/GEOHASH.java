package org.infinispan.server.resp.commands.geo;

import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.multimap.impl.EmbeddedMultimapSortedSetCache;
import org.infinispan.server.resp.AclCategory;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;

import io.netty.channel.ChannelHandlerContext;

/**
 * GEOHASH
 * <p>
 * Returns the Geohash strings representing the positions of the specified members.
 *
 * @see <a href="https://redis.io/commands/geohash/">GEOHASH</a>
 * @since 16.2
 */
public class GEOHASH extends RespCommand implements Resp3Command {

   public GEOHASH() {
      super(-3, 1, 1, 1, AclCategory.READ.mask() | AclCategory.GEO.mask() | AclCategory.SLOW.mask());
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler,
                                                      ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {

      byte[] key = arguments.get(0);
      List<byte[]> members = arguments.subList(1, arguments.size());
      EmbeddedMultimapSortedSetCache<byte[], byte[]> sortedSetCache = handler.getSortedSeMultimap();

      CompletionStage<List<Double>> scoresStage = sortedSetCache.scores(key, members);

      return handler.stageToReturn(scoresStage, ctx, (scores, writer) -> {
         writer.arrayStart(scores.size());
         for (Double score : scores) {
            if (score == null) {
               writer.nulls();
            } else {
               long geohash = GeoHashUtil.scoreToGeohash(score);
               String hashStr = GeoHashUtil.toBase32(geohash);
               writer.string(hashStr);
            }
         }
         writer.arrayEnd();
      });
   }
}
