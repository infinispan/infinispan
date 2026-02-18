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
 * GEODIST
 * <p>
 * Returns the distance between two members of a geospatial index.
 *
 * @see <a href="https://redis.io/commands/geodist/">GEODIST</a>
 * @since 16.2
 */
public class GEODIST extends RespCommand implements Resp3Command {

   public GEODIST() {
      super(-4, 1, 1, 1, AclCategory.READ.mask() | AclCategory.GEO.mask() | AclCategory.SLOW.mask());
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler,
                                                      ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {

      byte[] key = arguments.get(0);
      byte[] member1 = arguments.get(1);
      byte[] member2 = arguments.get(2);

      // Parse optional unit (default: M)
      GeoUnit unit = GeoUnit.M;
      if (arguments.size() > 3) {
         unit = GeoUnit.parse(arguments.get(3));
         if (unit == null) {
            handler.writer().customError("unsupported unit provided. please use M, KM, FT, MI");
            return handler.myStage();
         }
      }

      EmbeddedMultimapSortedSetCache<byte[], byte[]> sortedSetCache = handler.getSortedSeMultimap();

      // Get scores for both members
      CompletionStage<List<Double>> scoresStage = sortedSetCache.scores(key, List.of(member1, member2));

      GeoUnit finalUnit = unit;
      return handler.stageToReturn(scoresStage, ctx, (scores, writer) -> {
         Double score1 = scores.get(0);
         Double score2 = scores.get(1);

         if (score1 == null || score2 == null) {
            writer.nulls();
            return;
         }

         // Decode coordinates
         double[] coords1 = GeoHashUtil.decode(GeoHashUtil.scoreToGeohash(score1));
         double[] coords2 = GeoHashUtil.decode(GeoHashUtil.scoreToGeohash(score2));

         // Calculate distance
         double distanceMeters = GeoDistanceUtil.haversine(
               coords1[0], coords1[1], coords2[0], coords2[1]);
         double distance = GeoDistanceUtil.convertTo(distanceMeters, finalUnit);

         writer.doubles(distance);
      });
   }
}
