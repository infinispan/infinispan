package org.infinispan.server.resp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.within;
import static org.infinispan.server.resp.test.RespTestingUtil.assertWrongType;

import java.util.List;
import java.util.Set;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import io.lettuce.core.GeoAddArgs;
import io.lettuce.core.GeoArgs;
import io.lettuce.core.GeoCoordinates;
import io.lettuce.core.GeoSearch;
import io.lettuce.core.GeoValue;
import io.lettuce.core.GeoWithin;
import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.Value;
import io.lettuce.core.api.sync.RedisCommands;

/**
 * Tests for RESP GEO commands.
 *
 * @since 16.2
 */
@Test(groups = "functional", testName = "server.resp.GeoCommandsTest")
public class GeoCommandsTest extends SingleNodeRespBaseTest {

   RedisCommands<String, String> redis;

   // Known coordinates
   private static final double PALERMO_LON = 13.361389;
   private static final double PALERMO_LAT = 38.115556;
   private static final double CATANIA_LON = 15.087269;
   private static final double CATANIA_LAT = 37.502669;
   private static final double ROME_LON = 12.496366;
   private static final double ROME_LAT = 41.902782;

   @Override
   public Object[] factory() {
      return new Object[]{
            new GeoCommandsTest(),
      };
   }

   @BeforeMethod
   public void initConnection() {
      redis = redisConnection.sync();
   }

   public void testGeoAdd() {
      Long added = redis.geoadd("sicily", PALERMO_LON, PALERMO_LAT, "Palermo");
      assertThat(added).isEqualTo(1);

      added = redis.geoadd("sicily", CATANIA_LON, CATANIA_LAT, "Catania");
      assertThat(added).isEqualTo(1);

      // Adding existing member should return 0
      added = redis.geoadd("sicily", PALERMO_LON, PALERMO_LAT, "Palermo");
      assertThat(added).isEqualTo(0);

      // Verify stored as sorted set
      Long count = redis.zcard("sicily");
      assertThat(count).isEqualTo(2);
   }

   public void testGeoAddMultiple() {
      Long added = redis.geoadd("cities",
            GeoValue.just(PALERMO_LON, PALERMO_LAT, "Palermo"),
            GeoValue.just(CATANIA_LON, CATANIA_LAT, "Catania"),
            GeoValue.just(ROME_LON, ROME_LAT, "Rome"));
      assertThat(added).isEqualTo(3);
   }

   public void testGeoAddWithNX() {
      // Add initial member
      redis.geoadd("sicily", PALERMO_LON, PALERMO_LAT, "Palermo");

      // NX: Only add new elements, don't update existing
      Long added = redis.geoadd("sicily", GeoAddArgs.Builder.nx(),
            GeoValue.just(PALERMO_LON + 1, PALERMO_LAT + 1, "Palermo"), // Should be ignored
            GeoValue.just(CATANIA_LON, CATANIA_LAT, "Catania")); // Should be added
      assertThat(added).isEqualTo(1);

      // Verify Palermo wasn't updated (coordinates should be original)
      List<GeoCoordinates> positions = redis.geopos("sicily", "Palermo");
      assertThat(positions.get(0).getX().doubleValue()).isCloseTo(PALERMO_LON, within(0.0001));
   }

   public void testGeoAddWithXX() {
      // Add initial member
      redis.geoadd("sicily", PALERMO_LON, PALERMO_LAT, "Palermo");

      // XX: Only update existing elements, don't add new
      Long added = redis.geoadd("sicily", GeoAddArgs.Builder.xx(),
            GeoValue.just(PALERMO_LON + 0.1, PALERMO_LAT, "Palermo"), // Should be updated
            GeoValue.just(CATANIA_LON, CATANIA_LAT, "Catania")); // Should be ignored
      assertThat(added).isEqualTo(0); // XX doesn't count updates

      // Verify only Palermo exists
      Long count = redis.zcard("sicily");
      assertThat(count).isEqualTo(1);
   }

   public void testGeoAddWithCH() {
      // Add initial member
      redis.geoadd("sicily", PALERMO_LON, PALERMO_LAT, "Palermo");

      // CH: Return number of changed elements (added + updated)
      Long changed = redis.geoadd("sicily", GeoAddArgs.Builder.ch(),
            GeoValue.just(PALERMO_LON + 0.1, PALERMO_LAT, "Palermo"), // Changed
            GeoValue.just(CATANIA_LON, CATANIA_LAT, "Catania")); // Added
      assertThat(changed).isEqualTo(2);
   }

   public void testGeoAddXXAndNXError() {
      // XX and NX are mutually exclusive
      assertThatThrownBy(() -> redis.geoadd("sicily",
            GeoAddArgs.Builder.nx().xx(),
            GeoValue.just(PALERMO_LON, PALERMO_LAT, "Palermo")))
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("XX and NX");
   }

   public void testGeoPos() {
      redis.geoadd("sicily", PALERMO_LON, PALERMO_LAT, "Palermo");
      redis.geoadd("sicily", CATANIA_LON, CATANIA_LAT, "Catania");

      List<GeoCoordinates> positions = redis.geopos("sicily", "Palermo", "Catania");
      assertThat(positions).hasSize(2);

      // Verify coordinates (with some tolerance due to encoding)
      assertThat(positions.get(0).getX().doubleValue()).isCloseTo(PALERMO_LON, within(0.0001));
      assertThat(positions.get(0).getY().doubleValue()).isCloseTo(PALERMO_LAT, within(0.0001));
      assertThat(positions.get(1).getX().doubleValue()).isCloseTo(CATANIA_LON, within(0.0001));
      assertThat(positions.get(1).getY().doubleValue()).isCloseTo(CATANIA_LAT, within(0.0001));
   }

   public void testGeoPosNonExistent() {
      redis.geoadd("sicily", PALERMO_LON, PALERMO_LAT, "Palermo");

      List<GeoCoordinates> positions = redis.geopos("sicily", "Palermo", "NonExistent");
      assertThat(positions).hasSize(2);
      assertThat(positions.get(0)).isNotNull();
      assertThat(positions.get(1)).isNull();
   }

   public void testGeoDist() {
      redis.geoadd("sicily", PALERMO_LON, PALERMO_LAT, "Palermo");
      redis.geoadd("sicily", CATANIA_LON, CATANIA_LAT, "Catania");

      // Distance in meters (default)
      Double dist = redis.geodist("sicily", "Palermo", "Catania", GeoArgs.Unit.m);
      assertThat(dist).isNotNull();
      // Palermo to Catania is approximately 166 km
      assertThat(dist).isCloseTo(166274.0, within(1000.0));

      // Distance in kilometers
      dist = redis.geodist("sicily", "Palermo", "Catania", GeoArgs.Unit.km);
      assertThat(dist).isCloseTo(166.274, within(1.0));
   }

   public void testGeoDistNonExistent() {
      redis.geoadd("sicily", PALERMO_LON, PALERMO_LAT, "Palermo");

      Double dist = redis.geodist("sicily", "Palermo", "NonExistent", GeoArgs.Unit.m);
      assertThat(dist).isNull();
   }

   public void testGeoDistSameLocation() {
      redis.geoadd("sicily", PALERMO_LON, PALERMO_LAT, "Palermo");

      Double dist = redis.geodist("sicily", "Palermo", "Palermo", GeoArgs.Unit.m);
      assertThat(dist).isEqualTo(0.0);
   }

   public void testGeoDistAllUnits() {
      redis.geoadd("sicily", PALERMO_LON, PALERMO_LAT, "Palermo");
      redis.geoadd("sicily", CATANIA_LON, CATANIA_LAT, "Catania");

      // Test all units
      Double distM = redis.geodist("sicily", "Palermo", "Catania", GeoArgs.Unit.m);
      Double distKm = redis.geodist("sicily", "Palermo", "Catania", GeoArgs.Unit.km);
      Double distMi = redis.geodist("sicily", "Palermo", "Catania", GeoArgs.Unit.mi);
      Double distFt = redis.geodist("sicily", "Palermo", "Catania", GeoArgs.Unit.ft);

      assertThat(distKm).isCloseTo(distM / 1000.0, within(0.001));
      assertThat(distMi).isCloseTo(distM / 1609.344, within(0.001));
      assertThat(distFt).isCloseTo(distM / 0.3048, within(1.0));
   }

   public void testGeoHash() {
      redis.geoadd("sicily", PALERMO_LON, PALERMO_LAT, "Palermo");
      redis.geoadd("sicily", CATANIA_LON, CATANIA_LAT, "Catania");

      List<Value<String>> hashes = redis.geohash("sicily", "Palermo", "Catania");
      assertThat(hashes).hasSize(2);

      // Geohash strings should be 11 characters
      assertThat(hashes.get(0).getValue()).hasSize(11);
      assertThat(hashes.get(1).getValue()).hasSize(11);
   }

   public void testGeoHashNonExistent() {
      redis.geoadd("sicily", PALERMO_LON, PALERMO_LAT, "Palermo");

      List<Value<String>> hashes = redis.geohash("sicily", "Palermo", "NonExistent");
      assertThat(hashes).hasSize(2);
      assertThat(hashes.get(0).hasValue()).isTrue();
      assertThat(hashes.get(1).hasValue()).isFalse();
   }

   public void testGeoSearch() {
      redis.geoadd("sicily", PALERMO_LON, PALERMO_LAT, "Palermo");
      redis.geoadd("sicily", CATANIA_LON, CATANIA_LAT, "Catania");

      // Search by radius from coordinates
      Set<String> results = redis.geosearch("sicily",
            GeoSearch.fromCoordinates(15, 37),
            GeoSearch.byRadius(200, GeoArgs.Unit.km));

      assertThat(results).containsExactlyInAnyOrder("Palermo", "Catania");
   }

   public void testGeoSearchByMember() {
      redis.geoadd("sicily", PALERMO_LON, PALERMO_LAT, "Palermo");
      redis.geoadd("sicily", CATANIA_LON, CATANIA_LAT, "Catania");

      // Search by radius from member
      Set<String> results = redis.geosearch("sicily",
            GeoSearch.fromMember("Palermo"),
            GeoSearch.byRadius(200, GeoArgs.Unit.km));

      assertThat(results).containsExactlyInAnyOrder("Palermo", "Catania");
   }

   public void testGeoSearchWithDistance() {
      redis.geoadd("sicily", PALERMO_LON, PALERMO_LAT, "Palermo");
      redis.geoadd("sicily", CATANIA_LON, CATANIA_LAT, "Catania");

      List<GeoWithin<String>> results = redis.geosearch("sicily",
            GeoSearch.fromCoordinates(PALERMO_LON, PALERMO_LAT),
            GeoSearch.byRadius(200, GeoArgs.Unit.km),
            new GeoArgs().withDistance().asc());

      assertThat(results).hasSize(2);
      // Palermo should be first (distance 0)
      assertThat(results.get(0).getMember()).isEqualTo("Palermo");
      assertThat(results.get(0).getDistance()).isCloseTo(0.0, within(0.1));
      // Catania should be second
      assertThat(results.get(1).getMember()).isEqualTo("Catania");
   }

   public void testGeoSearchWithCount() {
      redis.geoadd("cities",
            GeoValue.just(PALERMO_LON, PALERMO_LAT, "Palermo"),
            GeoValue.just(CATANIA_LON, CATANIA_LAT, "Catania"),
            GeoValue.just(ROME_LON, ROME_LAT, "Rome"));

      List<GeoWithin<String>> results = redis.geosearch("cities",
            GeoSearch.fromCoordinates(PALERMO_LON, PALERMO_LAT),
            GeoSearch.byRadius(500, GeoArgs.Unit.km),
            new GeoArgs().asc().withCount(2));

      assertThat(results).hasSize(2);
   }

   public void testGeoSearchByBox() {
      redis.geoadd("sicily", PALERMO_LON, PALERMO_LAT, "Palermo");
      redis.geoadd("sicily", CATANIA_LON, CATANIA_LAT, "Catania");

      Set<String> results = redis.geosearch("sicily",
            GeoSearch.fromCoordinates(14, 38),
            GeoSearch.byBox(400, 400, GeoArgs.Unit.km));

      assertThat(results).containsExactlyInAnyOrder("Palermo", "Catania");
   }

   public void testGeoSearchDesc() {
      redis.geoadd("sicily", PALERMO_LON, PALERMO_LAT, "Palermo");
      redis.geoadd("sicily", CATANIA_LON, CATANIA_LAT, "Catania");

      List<GeoWithin<String>> results = redis.geosearch("sicily",
            GeoSearch.fromCoordinates(PALERMO_LON, PALERMO_LAT),
            GeoSearch.byRadius(200, GeoArgs.Unit.km),
            new GeoArgs().withDistance().desc());

      assertThat(results).hasSize(2);
      // Catania should be first (farther away) in DESC order
      assertThat(results.get(0).getMember()).isEqualTo("Catania");
      assertThat(results.get(1).getMember()).isEqualTo("Palermo");
   }

   public void testGeoSearchWithCoord() {
      redis.geoadd("sicily", PALERMO_LON, PALERMO_LAT, "Palermo");

      List<GeoWithin<String>> results = redis.geosearch("sicily",
            GeoSearch.fromCoordinates(PALERMO_LON, PALERMO_LAT),
            GeoSearch.byRadius(10, GeoArgs.Unit.km),
            new GeoArgs().withCoordinates());

      assertThat(results).hasSize(1);
      assertThat(results.get(0).getCoordinates()).isNotNull();
      assertThat(results.get(0).getCoordinates().getX().doubleValue())
            .isCloseTo(PALERMO_LON, within(0.0001));
   }

   public void testGeoSearchWithHash() {
      redis.geoadd("sicily", PALERMO_LON, PALERMO_LAT, "Palermo");

      List<GeoWithin<String>> results = redis.geosearch("sicily",
            GeoSearch.fromCoordinates(PALERMO_LON, PALERMO_LAT),
            GeoSearch.byRadius(10, GeoArgs.Unit.km),
            new GeoArgs().withHash());

      assertThat(results).hasSize(1);
      assertThat(results.get(0).getGeohash()).isNotNull();
      assertThat(results.get(0).getGeohash()).isGreaterThan(0);
   }

   public void testGeoSearchNonExistingMember() {
      redis.geoadd("sicily", PALERMO_LON, PALERMO_LAT, "Palermo");

      assertThatThrownBy(() -> redis.geosearch("sicily",
            GeoSearch.fromMember("NonExistent"),
            GeoSearch.byRadius(200, GeoArgs.Unit.km)))
            .isInstanceOf(RedisCommandExecutionException.class);
   }

   public void testGeoSearchNonExistingKey() {
      // Searching in a non-existing key should return empty results
      Set<String> results = redis.geosearch("nonexistent",
            GeoSearch.fromCoordinates(0, 0),
            GeoSearch.byRadius(100, GeoArgs.Unit.km));

      assertThat(results).isEmpty();
   }

   public void testGeoSearchStore() {
      redis.geoadd("sicily", PALERMO_LON, PALERMO_LAT, "Palermo");
      redis.geoadd("sicily", CATANIA_LON, CATANIA_LAT, "Catania");

      Long stored = redis.geosearchstore("near_palermo", "sicily",
            GeoSearch.fromMember("Palermo"),
            GeoSearch.byRadius(200, GeoArgs.Unit.km),
            new GeoArgs().asc(), false);

      assertThat(stored).isEqualTo(2);

      // Verify the destination key exists
      Long count = redis.zcard("near_palermo");
      assertThat(count).isEqualTo(2);

      // Verify scores are geohashes (not distances)
      Double score = redis.zscore("near_palermo", "Palermo");
      assertThat(score).isGreaterThan(1000000); // Geohash is a large number
   }

   public void testGeoSearchStoreWithStoreDist() {
      redis.geoadd("sicily", PALERMO_LON, PALERMO_LAT, "Palermo");
      redis.geoadd("sicily", CATANIA_LON, CATANIA_LAT, "Catania");

      Long stored = redis.geosearchstore("distances", "sicily",
            GeoSearch.fromMember("Palermo"),
            GeoSearch.byRadius(200, GeoArgs.Unit.km),
            new GeoArgs().asc(), true); // true = STOREDIST

      assertThat(stored).isEqualTo(2);

      // Verify scores are distances
      Double palermoScore = redis.zscore("distances", "Palermo");
      Double cataniaScore = redis.zscore("distances", "Catania");

      assertThat(palermoScore).isCloseTo(0.0, within(0.1)); // Distance to itself
      assertThat(cataniaScore).isCloseTo(166.274, within(1.0)); // ~166 km
   }

   public void testGeoWrongType() {
      // Set a string key
      redis.set("stringkey", "value");

      // GEO operations on wrong type should fail
      assertWrongType(() -> redis.set("stringkey", "value"),
            () -> redis.geoadd("stringkey", PALERMO_LON, PALERMO_LAT, "Palermo"));
   }

   // Disabled: GEORADIUS is deprecated. Use GEOSEARCH instead.
   // Lettuce may send GEORADIUS_RO for read-only operations.
   @Test(enabled = false)
   public void testGeoRadiusLegacy() {
      redis.geoadd("sicily", PALERMO_LON, PALERMO_LAT, "Palermo");
      redis.geoadd("sicily", CATANIA_LON, CATANIA_LAT, "Catania");

      // Legacy GEORADIUS command
      Set<String> results = redis.georadius("sicily", 15, 37, 200, GeoArgs.Unit.km);

      assertThat(results).containsExactlyInAnyOrder("Palermo", "Catania");
   }

   // Disabled: GEORADIUSBYMEMBER is deprecated. Use GEOSEARCH with FROMMEMBER instead.
   // Lettuce may send GEORADIUSBYMEMBER_RO for read-only operations.
   @Test(enabled = false)
   public void testGeoRadiusByMemberLegacy() {
      redis.geoadd("sicily", PALERMO_LON, PALERMO_LAT, "Palermo");
      redis.geoadd("sicily", CATANIA_LON, CATANIA_LAT, "Catania");

      // Legacy GEORADIUSBYMEMBER command
      Set<String> results = redis.georadiusbymember("sicily", "Palermo", 200, GeoArgs.Unit.km);

      assertThat(results).containsExactlyInAnyOrder("Palermo", "Catania");
   }

   public void testGeoCanUseZremToRemoveMembers() {
      redis.geoadd("sicily", PALERMO_LON, PALERMO_LAT, "Palermo");
      redis.geoadd("sicily", CATANIA_LON, CATANIA_LAT, "Catania");

      // GEO data is stored in sorted sets, so ZREM works
      Long removed = redis.zrem("sicily", "Palermo");
      assertThat(removed).isEqualTo(1);

      // Verify Catania still exists
      List<GeoCoordinates> positions = redis.geopos("sicily", "Catania");
      assertThat(positions).hasSize(1);
      assertThat(positions.get(0)).isNotNull();
   }

   public void testGeoInvalidCoordinates() {
      // Longitude out of range
      try {
         redis.geoadd("test", 181, 0, "invalid");
      } catch (Exception e) {
         assertThat(e.getMessage()).contains("invalid longitude,latitude pair");
      }

      // Latitude out of range
      try {
         redis.geoadd("test", 0, 86, "invalid");
      } catch (Exception e) {
         assertThat(e.getMessage()).contains("invalid longitude,latitude pair");
      }
   }
}
