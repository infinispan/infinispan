package org.infinispan.server.resp.commands.geo;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
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
import org.infinispan.server.resp.commands.ArgumentUtils;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.serialization.ResponseWriter;

import io.netty.channel.ChannelHandlerContext;

/**
 * GEORADIUS
 * <p>
 * Returns members of a geospatial index that are within a given radius.
 * <p>
 * <b>Deprecated:</b> Use GEOSEARCH instead.
 *
 * @see <a href="https://redis.io/commands/georadius/">GEORADIUS</a>
 * @since 16.2
 */
public class GEORADIUS extends RespCommand implements Resp3Command {

   public GEORADIUS() {
      super(-6, 1, 1, 1, AclCategory.WRITE.mask() | AclCategory.GEO.mask() | AclCategory.SLOW.mask());
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler,
                                                      ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {

      // GEORADIUS key longitude latitude radius unit [WITHCOORD] [WITHDIST] [WITHHASH] [COUNT count [ANY]] [ASC|DESC] [STORE key] [STOREDIST key]
      byte[] key = arguments.get(0);

      double longitude;
      double latitude;
      double radius;
      try {
         longitude = ArgumentUtils.toDouble(arguments.get(1));
         latitude = ArgumentUtils.toDouble(arguments.get(2));
         radius = ArgumentUtils.toDouble(arguments.get(3));
      } catch (NumberFormatException e) {
         handler.writer().valueNotAValidFloat();
         return handler.myStage();
      }

      if (!GeoHashUtil.isValidLongitude(longitude) || !GeoHashUtil.isValidLatitude(latitude)) {
         handler.writer().customError(String.format(
               "invalid longitude,latitude pair %.6f,%.6f", longitude, latitude));
         return handler.myStage();
      }

      GeoUnit unit = GeoUnit.parse(arguments.get(4));
      if (unit == null) {
         handler.writer().customError("unsupported unit provided. please use M, KM, FT, MI");
         return handler.myStage();
      }

      // Parse optional arguments
      boolean withCoord = false;
      boolean withDist = false;
      boolean withHash = false;
      Long count = null;
      boolean any = false;
      boolean asc = true;
      byte[] storeKey = null;
      boolean storeDist = false;

      int pos = 5;
      while (pos < arguments.size()) {
         String arg = new String(arguments.get(pos)).toUpperCase();
         switch (arg) {
            case "WITHCOORD" -> withCoord = true;
            case "WITHDIST" -> withDist = true;
            case "WITHHASH" -> withHash = true;
            case "ASC" -> asc = true;
            case "DESC" -> asc = false;
            case "COUNT" -> {
               if (pos + 1 >= arguments.size()) {
                  handler.writer().syntaxError();
                  return handler.myStage();
               }
               try {
                  count = ArgumentUtils.toLong(arguments.get(++pos));
               } catch (NumberFormatException e) {
                  handler.writer().valueNotInteger();
                  return handler.myStage();
               }
               // Check for optional ANY
               if (pos + 1 < arguments.size() &&
                     new String(arguments.get(pos + 1)).equalsIgnoreCase("ANY")) {
                  any = true;
                  pos++;
               }
            }
            case "STORE" -> {
               if (pos + 1 >= arguments.size()) {
                  handler.writer().syntaxError();
                  return handler.myStage();
               }
               storeKey = arguments.get(++pos);
            }
            case "STOREDIST" -> {
               if (pos + 1 >= arguments.size()) {
                  handler.writer().syntaxError();
                  return handler.myStage();
               }
               storeKey = arguments.get(++pos);
               storeDist = true;
            }
            default -> {
               handler.writer().syntaxError();
               return handler.myStage();
            }
         }
         pos++;
      }

      // Cannot use WITH* options with STORE
      if (storeKey != null && (withCoord || withDist || withHash)) {
         handler.writer().customError("STORE option in GEORADIUS is not compatible with WITHDIST, WITHHASH and WITHCOORD options");
         return handler.myStage();
      }

      EmbeddedMultimapSortedSetCache<byte[], byte[]> sortedSetCache = handler.getSortedSeMultimap();

      double[] center = new double[]{longitude, latitude};
      double radiusMeters = unit.toMeters(radius);

      // Get all entries
      SortedSetSubsetArgs.Builder<Long> builder = SortedSetSubsetArgs.create();
      builder.start(0L).stop(-1L);

      boolean finalWithCoord = withCoord;
      boolean finalWithDist = withDist;
      boolean finalWithHash = withHash;
      boolean finalAsc = asc;
      Long finalCount = count;
      byte[] finalStoreKey = storeKey;
      boolean finalStoreDist = storeDist;

      return sortedSetCache.subsetByIndex(key, builder.build())
            .thenCompose(entries -> {
               List<GEOSEARCH.GeoResult> results = filterByRadius(entries, center, radiusMeters);

               // Sort
               Comparator<GEOSEARCH.GeoResult> comparator =
                     Comparator.comparingDouble(GEOSEARCH.GeoResult::distanceMeters);
               if (!finalAsc) {
                  comparator = comparator.reversed();
               }
               results.sort(comparator);

               // Apply count
               if (finalCount != null && finalCount > 0 && results.size() > finalCount) {
                  results = new ArrayList<>(results.subList(0, finalCount.intValue()));
               }

               if (finalStoreKey != null) {
                  // Store results
                  return storeResults(handler, ctx, sortedSetCache, finalStoreKey,
                        results, finalStoreDist, unit);
               }

               // Return results
               return handler.stageToReturn(CompletableFuture.completedFuture(results), ctx,
                     createResponseWriter(finalWithCoord, finalWithDist, finalWithHash, unit));
            });
   }

   private List<GEOSEARCH.GeoResult> filterByRadius(Collection<ScoredValue<byte[]>> entries,
                                                    double[] center, double radiusMeters) {
      List<GEOSEARCH.GeoResult> results = new ArrayList<>();

      for (ScoredValue<byte[]> entry : entries) {
         double[] coords = GeoHashUtil.decode(GeoHashUtil.scoreToGeohash(entry.score()));
         double distance = GeoDistanceUtil.haversine(
               center[0], center[1], coords[0], coords[1]);

         if (distance <= radiusMeters) {
            results.add(new GEOSEARCH.GeoResult(
                  entry.getValue(),
                  coords[0], coords[1],
                  distance,
                  entry.score()));
         }
      }

      return results;
   }

   private CompletionStage<RespRequestHandler> storeResults(Resp3Handler handler,
                                                            ChannelHandlerContext ctx,
                                                            EmbeddedMultimapSortedSetCache<byte[], byte[]> sortedSetCache,
                                                            byte[] storeKey,
                                                            List<GEOSEARCH.GeoResult> results,
                                                            boolean storeDist,
                                                            GeoUnit unit) {

      List<ScoredValue<byte[]>> scoredValues = new ArrayList<>(results.size());
      for (GEOSEARCH.GeoResult result : results) {
         double score = storeDist
               ? GeoDistanceUtil.convertTo(result.distanceMeters(), unit)
               : result.score();
         scoredValues.add(ScoredValue.of(score, result.member()));
      }

      if (scoredValues.isEmpty()) {
         return handler.stageToReturn(CompletableFuture.completedFuture(0L),
               ctx, ResponseWriter.INTEGER);
      }

      SortedSetAddArgs addArgs = SortedSetAddArgs.create().replace().build();
      return handler.stageToReturn(sortedSetCache.addMany(storeKey, scoredValues, addArgs),
            ctx, ResponseWriter.INTEGER);
   }

   private java.util.function.BiConsumer<List<GEOSEARCH.GeoResult>, ResponseWriter> createResponseWriter(
         boolean withCoord, boolean withDist, boolean withHash, GeoUnit unit) {
      return (results, writer) -> {
         writer.arrayStart(results.size());
         for (GEOSEARCH.GeoResult result : results) {
            if (withCoord || withDist || withHash) {
               int elements = 1;
               if (withDist) elements++;
               if (withHash) elements++;
               if (withCoord) elements++;

               writer.arrayStart(elements);
               writer.string(result.member());

               if (withDist) {
                  writer.doubles(GeoDistanceUtil.convertTo(result.distanceMeters(), unit));
               }
               if (withHash) {
                  writer.integers((long) result.score());
               }
               if (withCoord) {
                  writer.arrayStart(2);
                  writer.doubles(result.longitude());
                  writer.doubles(result.latitude());
                  writer.arrayEnd();
               }
               writer.arrayEnd();
            } else {
               writer.string(result.member());
            }
         }
         writer.arrayEnd();
      };
   }
}
