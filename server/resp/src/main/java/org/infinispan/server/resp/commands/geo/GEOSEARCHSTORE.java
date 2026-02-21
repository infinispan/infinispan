package org.infinispan.server.resp.commands.geo;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.infinispan.multimap.impl.EmbeddedMultimapSortedSetCache;
import org.infinispan.multimap.impl.ScoredValue;
import org.infinispan.multimap.impl.SortedSetAddArgs;
import org.infinispan.multimap.impl.SortedSetSubsetArgs;
import org.infinispan.server.resp.AclCategory;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.serialization.ResponseWriter;

import io.netty.channel.ChannelHandlerContext;

/**
 * GEOSEARCHSTORE
 * <p>
 * Stores members of a geospatial index that are within a given geographic area
 * in a new sorted set.
 *
 * @see <a href="https://redis.io/commands/geosearchstore/">GEOSEARCHSTORE</a>
 * @since 16.2
 */
public class GEOSEARCHSTORE extends RespCommand implements Resp3Command {

   public GEOSEARCHSTORE() {
      super(-8, 1, 2, 1, AclCategory.WRITE.mask() | AclCategory.GEO.mask() | AclCategory.SLOW.mask());
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler,
                                                      ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {

      byte[] destination = arguments.get(0);
      byte[] source = arguments.get(1);

      // Check for STOREDIST option
      boolean storeDist = false;
      for (int i = 2; i < arguments.size(); i++) {
         if (new String(arguments.get(i)).equalsIgnoreCase("STOREDIST")) {
            storeDist = true;
            break;
         }
      }

      // Parse options (skip destination and source)
      GeoSearchOptions options = GeoSearchOptions.parse(arguments, 2);

      // Validate required options
      if (!options.hasCenter()) {
         handler.writer().customError("exactly one of FROMMEMBER or FROMLONLAT must be specified");
         return handler.myStage();
      }
      if (!options.hasShape()) {
         handler.writer().customError("exactly one of BYRADIUS or BYBOX must be specified");
         return handler.myStage();
      }

      EmbeddedMultimapSortedSetCache<byte[], byte[]> sortedSetCache = handler.getSortedSeMultimap();

      // Resolve center coordinates
      CompletionStage<double[]> centerStage;
      if (options.getFromMember() != null) {
         centerStage = sortedSetCache.score(source, options.getFromMember())
               .thenApply(score -> {
                  if (score == null) {
                     return null;
                  }
                  return GeoHashUtil.decode(GeoHashUtil.scoreToGeohash(score));
               });
      } else {
         centerStage = CompletableFuture.completedFuture(
               new double[]{options.getLongitude(), options.getLatitude()});
      }

      boolean finalStoreDist = storeDist;
      return centerStage.thenCompose(center -> {
         if (center == null) {
            handler.writer().customError("could not decode requested zset member");
            return handler.myStage();
         }

         // Get all entries from the source sorted set
         SortedSetSubsetArgs.Builder<Long> builder = SortedSetSubsetArgs.create();
         builder.start(0L).stop(-1L);

         return sortedSetCache.subsetByIndex(source, builder.build())
               .thenCompose(entries -> {
                  List<GEOSEARCH.GeoResult> results = filterAndSort(entries, center, options);

                  // Convert results to ScoredValues for storage
                  List<ScoredValue<byte[]>> scoredValues = new ArrayList<>(results.size());
                  for (GEOSEARCH.GeoResult result : results) {
                     double score;
                     if (finalStoreDist) {
                        // Store distance in the specified unit
                        score = GeoDistanceUtil.convertTo(result.distanceMeters(), options.getUnit());
                     } else {
                        // Store original geohash score
                        score = result.score();
                     }
                     scoredValues.add(ScoredValue.of(score, result.member()));
                  }

                  if (scoredValues.isEmpty()) {
                     // Delete destination if no results
                     return sortedSetCache.removeAll(destination, List.of())
                           .thenApply(v -> 0L);
                  }

                  // Store results with replace
                  SortedSetAddArgs addArgs = SortedSetAddArgs.create().replace().build();
                  return sortedSetCache.addMany(destination, scoredValues, addArgs);
               })
               .thenCompose(count -> handler.stageToReturn(
                     CompletableFuture.completedFuture(count),
                     ctx,
                     ResponseWriter.INTEGER));
      });
   }

   /**
    * Filter entries by geographic criteria and sort by distance.
    */
   private List<GEOSEARCH.GeoResult> filterAndSort(Collection<ScoredValue<byte[]>> entries,
                                                   double[] center, GeoSearchOptions options) {
      List<GEOSEARCH.GeoResult> results = new ArrayList<>();

      for (ScoredValue<byte[]> entry : entries) {
         double[] coords = GeoHashUtil.decode(GeoHashUtil.scoreToGeohash(entry.score()));
         double distanceMeters = GeoDistanceUtil.haversine(
               center[0], center[1], coords[0], coords[1]);

         boolean matches;
         if (options.isByRadius()) {
            double radiusMeters = options.getUnit().toMeters(options.getRadius());
            matches = distanceMeters <= radiusMeters;
         } else {
            matches = GeoDistanceUtil.withinBox(
                  center[0], center[1], coords[0], coords[1],
                  options.getWidth(), options.getHeight(), options.getUnit());
         }

         if (matches) {
            results.add(new GEOSEARCH.GeoResult(
                  entry.getValue(),
                  coords[0], coords[1],
                  distanceMeters,
                  entry.score()));
         }
      }

      // Sort by distance
      java.util.Comparator<GEOSEARCH.GeoResult> comparator =
            java.util.Comparator.comparingDouble(GEOSEARCH.GeoResult::distanceMeters);
      if (!options.isAsc()) {
         comparator = comparator.reversed();
      }
      results.sort(comparator);

      // Apply COUNT limit
      if (options.getCount() != null && options.getCount() > 0) {
         int limit = options.getCount().intValue();
         if (results.size() > limit) {
            results = new ArrayList<>(results.subList(0, limit));
         }
      }

      return results;
   }
}
