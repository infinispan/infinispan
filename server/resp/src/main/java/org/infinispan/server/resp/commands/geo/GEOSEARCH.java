package org.infinispan.server.resp.commands.geo;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.infinispan.multimap.impl.EmbeddedMultimapSortedSetCache;
import org.infinispan.multimap.impl.ScoredValue;
import org.infinispan.multimap.impl.SortedSetSubsetArgs;
import org.infinispan.server.resp.AclCategory;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.serialization.ResponseWriter;

import io.netty.channel.ChannelHandlerContext;

/**
 * GEOSEARCH
 * <p>
 * Returns members of a geospatial index that are within a given geographic area.
 *
 * @see <a href="https://redis.io/commands/geosearch/">GEOSEARCH</a>
 * @since 16.2
 */
public class GEOSEARCH extends RespCommand implements Resp3Command {

   public GEOSEARCH() {
      super(-7, 1, 1, 1, AclCategory.READ.mask() | AclCategory.GEO.mask() | AclCategory.SLOW.mask());
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler,
                                                      ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {

      byte[] key = arguments.get(0);

      // Parse options
      GeoSearchOptions options = GeoSearchOptions.parse(arguments, 1);

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
         centerStage = sortedSetCache.score(key, options.getFromMember())
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

      return centerStage.thenCompose(center -> {
         if (center == null) {
            // Member not found
            handler.writer().customError("could not decode requested zset member");
            return handler.myStage();
         }

         // Get all entries from the sorted set
         SortedSetSubsetArgs.Builder<Long> builder = SortedSetSubsetArgs.create();
         builder.start(0L).stop(-1L);

         return sortedSetCache.subsetByIndex(key, builder.build())
               .thenCompose(entries -> {
                  List<GeoResult> results = filterAndSort(entries, center, options);
                  return handler.stageToReturn(CompletableFuture.completedFuture(results), ctx,
                        createResponseWriter(options));
               });
      });
   }

   /**
    * Filter entries by geographic criteria and sort by distance.
    */
   protected List<GeoResult> filterAndSort(Collection<ScoredValue<byte[]>> entries,
                                           double[] center, GeoSearchOptions options) {
      List<GeoResult> results = new ArrayList<>();

      for (ScoredValue<byte[]> entry : entries) {
         double[] coords = GeoHashUtil.decode(GeoHashUtil.scoreToGeohash(entry.score()));
         double distanceMeters = GeoDistanceUtil.haversine(
               center[0], center[1], coords[0], coords[1]);

         boolean matches;
         if (options.isByRadius()) {
            double radiusMeters = options.getUnit().toMeters(options.getRadius());
            matches = distanceMeters <= radiusMeters;
         } else {
            // BYBOX
            matches = GeoDistanceUtil.withinBox(
                  center[0], center[1], coords[0], coords[1],
                  options.getWidth(), options.getHeight(), options.getUnit());
         }

         if (matches) {
            results.add(new GeoResult(
                  entry.getValue(),
                  coords[0], coords[1],
                  distanceMeters,
                  entry.score()));
         }
      }

      // Sort by distance
      Comparator<GeoResult> comparator = Comparator.comparingDouble(GeoResult::distanceMeters);
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

   /**
    * Create response writer based on WITH* flags.
    */
   protected java.util.function.BiConsumer<List<GeoResult>, ResponseWriter> createResponseWriter(GeoSearchOptions options) {
      boolean withCoord = options.isWithCoord();
      boolean withDist = options.isWithDist();
      boolean withHash = options.isWithHash();
      GeoUnit unit = options.getUnit();

      return (results, writer) -> {
         writer.arrayStart(results.size());
         for (GeoResult result : results) {
            if (withCoord || withDist || withHash) {
               // Array response: [member, [dist], [hash], [[lon, lat]]]
               int elements = 1;
               if (withDist) elements++;
               if (withHash) elements++;
               if (withCoord) elements++;

               writer.arrayStart(elements);
               writer.string(result.member);

               if (withDist) {
                  double distance = GeoDistanceUtil.convertTo(result.distanceMeters, unit);
                  writer.doubles(distance);
               }
               if (withHash) {
                  writer.integers((long) result.score);
               }
               if (withCoord) {
                  writer.arrayStart(2);
                  writer.doubles(result.longitude);
                  writer.doubles(result.latitude);
                  writer.arrayEnd();
               }
               writer.arrayEnd();
            } else {
               // Simple member response
               writer.string(result.member);
            }
         }
         writer.arrayEnd();
      };
   }

   /**
    * Result of a geo search.
    */
   public record GeoResult(byte[] member, double longitude, double latitude,
                           double distanceMeters, double score) {
   }
}
