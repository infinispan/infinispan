package org.infinispan.server.resp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.within;
import static org.infinispan.test.TestingUtil.k;

import java.util.List;

import org.testng.annotations.Test;

import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.output.ArrayOutput;
import io.lettuce.core.output.StatusOutput;
import io.lettuce.core.protocol.CommandArgs;

@Test(groups = "functional", testName = "server.resp.TDigestTest")
public class TDigestTest extends SingleNodeRespBaseTest {

   @Test
   public void testTDigestCreate() {
      RedisCommands<String, String> redis = redisConnection.sync();

      String result = redis.dispatch(
            command("TDIGEST.CREATE"),
            new StatusOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()));
      assertThat(result).isEqualTo("OK");

      // Try to create the same key again - should fail
      assertThatThrownBy(() -> redis.dispatch(
            command("TDIGEST.CREATE"),
            new StatusOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k())))
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("item exists");
   }

   @Test
   public void testTDigestCreateWithCompression() {
      RedisCommands<String, String> redis = redisConnection.sync();

      String result = redis.dispatch(
            command("TDIGEST.CREATE"),
            new StatusOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("COMPRESSION").add("200"));
      assertThat(result).isEqualTo("OK");
   }

   @Test
   public void testTDigestAdd() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // Create t-digest
      redis.dispatch(
            command("TDIGEST.CREATE"),
            new StatusOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()));

      // Add values
      String result = redis.dispatch(
            command("TDIGEST.ADD"),
            new StatusOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("1").add("2").add("3").add("4").add("5"));
      assertThat(result).isEqualTo("OK");
   }

   @Test
   public void testTDigestAddOnNonExistent() {
      RedisCommands<String, String> redis = redisConnection.sync();

      assertThatThrownBy(() -> redis.dispatch(
            command("TDIGEST.ADD"),
            new StatusOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("1")))
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("not found");
   }

   @Test
   public void testTDigestReset() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // Create and add values
      redis.dispatch(
            command("TDIGEST.CREATE"),
            new StatusOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()));
      redis.dispatch(
            command("TDIGEST.ADD"),
            new StatusOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("1").add("2").add("3"));

      // Reset
      String result = redis.dispatch(
            command("TDIGEST.RESET"),
            new StatusOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()));
      assertThat(result).isEqualTo("OK");
   }

   @Test
   public void testTDigestMinMax() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // Create and add values
      redis.dispatch(
            command("TDIGEST.CREATE"),
            new StatusOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()));
      redis.dispatch(
            command("TDIGEST.ADD"),
            new StatusOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("5").add("10").add("15").add("20").add("25"));

      // Get min
      List<Object> minResult = redis.dispatch(
            command("TDIGEST.MIN"),
            new ArrayOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()));
      // Output is a double, which may come as string in the array
      assertThat(Double.parseDouble(minResult.get(0).toString())).isCloseTo(5.0, within(0.001));

      // Get max
      List<Object> maxResult = redis.dispatch(
            command("TDIGEST.MAX"),
            new ArrayOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()));
      assertThat(Double.parseDouble(maxResult.get(0).toString())).isCloseTo(25.0, within(0.001));
   }

   @Test
   public void testTDigestQuantile() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // Create and add values (1-100)
      redis.dispatch(
            command("TDIGEST.CREATE"),
            new StatusOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()));

      CommandArgs<String, String> addArgs = new CommandArgs<>(StringCodec.UTF8).addKey(k());
      for (int i = 1; i <= 100; i++) {
         addArgs.add(String.valueOf(i));
      }
      redis.dispatch(command("TDIGEST.ADD"), new StatusOutput<>(StringCodec.UTF8), addArgs);

      // Get 50th percentile (median) - should be around 50
      List<Object> result = redis.dispatch(
            command("TDIGEST.QUANTILE"),
            new ArrayOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("0.5"));
      double median = Double.parseDouble(result.get(0).toString());
      assertThat(median).isCloseTo(50.0, within(10.0)); // Allow some error
   }

   @Test
   public void testTDigestCdf() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // Create and add values (1-100)
      redis.dispatch(
            command("TDIGEST.CREATE"),
            new StatusOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()));

      CommandArgs<String, String> addArgs = new CommandArgs<>(StringCodec.UTF8).addKey(k());
      for (int i = 1; i <= 100; i++) {
         addArgs.add(String.valueOf(i));
      }
      redis.dispatch(command("TDIGEST.ADD"), new StatusOutput<>(StringCodec.UTF8), addArgs);

      // CDF of 50 should be around 0.5
      List<Object> result = redis.dispatch(
            command("TDIGEST.CDF"),
            new ArrayOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("50"));
      double cdf = Double.parseDouble(result.get(0).toString());
      assertThat(cdf).isCloseTo(0.5, within(0.1));
   }

   @Test
   public void testTDigestRank() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // Create and add values
      redis.dispatch(
            command("TDIGEST.CREATE"),
            new StatusOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()));
      redis.dispatch(
            command("TDIGEST.ADD"),
            new StatusOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("1").add("2").add("3").add("4").add("5"));

      // Rank of 3 should be around 3 (middle)
      List<Object> result = redis.dispatch(
            command("TDIGEST.RANK"),
            new ArrayOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("3"));
      assertThat(result).hasSize(1);
   }

   @Test
   public void testTDigestRevRank() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // Create and add values
      redis.dispatch(
            command("TDIGEST.CREATE"),
            new StatusOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()));
      redis.dispatch(
            command("TDIGEST.ADD"),
            new StatusOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("1").add("2").add("3").add("4").add("5"));

      // RevRank
      List<Object> result = redis.dispatch(
            command("TDIGEST.REVRANK"),
            new ArrayOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("3"));
      assertThat(result).hasSize(1);
   }

   @Test
   public void testTDigestByRank() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // Create and add values
      redis.dispatch(
            command("TDIGEST.CREATE"),
            new StatusOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()));
      redis.dispatch(
            command("TDIGEST.ADD"),
            new StatusOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("10").add("20").add("30").add("40").add("50"));

      // Value at rank 0 should be min (10)
      List<Object> result = redis.dispatch(
            command("TDIGEST.BYRANK"),
            new ArrayOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("0"));
      double value = Double.parseDouble(result.get(0).toString());
      assertThat(value).isCloseTo(10.0, within(1.0));
   }

   @Test
   public void testTDigestByRevRank() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // Create and add values
      redis.dispatch(
            command("TDIGEST.CREATE"),
            new StatusOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()));
      redis.dispatch(
            command("TDIGEST.ADD"),
            new StatusOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("10").add("20").add("30").add("40").add("50"));

      // Value at reverse rank 0 should be max (50)
      List<Object> result = redis.dispatch(
            command("TDIGEST.BYREVRANK"),
            new ArrayOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("0"));
      double value = Double.parseDouble(result.get(0).toString());
      assertThat(value).isCloseTo(50.0, within(1.0));
   }

   @Test
   public void testTDigestTrimmedMean() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // Create and add values (1-100)
      redis.dispatch(
            command("TDIGEST.CREATE"),
            new StatusOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()));

      CommandArgs<String, String> addArgs = new CommandArgs<>(StringCodec.UTF8).addKey(k());
      for (int i = 1; i <= 100; i++) {
         addArgs.add(String.valueOf(i));
      }
      redis.dispatch(command("TDIGEST.ADD"), new StatusOutput<>(StringCodec.UTF8), addArgs);

      // Trimmed mean between 0.1 and 0.9 should exclude outliers
      List<Object> result = redis.dispatch(
            command("TDIGEST.TRIMMED_MEAN"),
            new ArrayOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("0.1").add("0.9"));
      double mean = Double.parseDouble(result.get(0).toString());
      assertThat(mean).isCloseTo(50.0, within(10.0));
   }

   @Test
   public void testTDigestInfo() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // Create with specific compression
      redis.dispatch(
            command("TDIGEST.CREATE"),
            new StatusOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("COMPRESSION").add("200"));

      // Add some values
      redis.dispatch(
            command("TDIGEST.ADD"),
            new StatusOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("1").add("2").add("3"));

      // Get info
      List<Object> info = redis.dispatch(
            command("TDIGEST.INFO"),
            new ArrayOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()));

      // Should have key-value pairs
      assertThat(info.size()).isGreaterThanOrEqualTo(4);
      assertThat(info.get(0)).isEqualTo("Compression");
      assertThat(info.get(1)).isEqualTo(200L);
   }

   @Test
   public void testTDigestInfoNonExistent() {
      RedisCommands<String, String> redis = redisConnection.sync();

      assertThatThrownBy(() -> redis.dispatch(
            command("TDIGEST.INFO"),
            new ArrayOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k())))
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("not found");
   }

   @Test
   public void testTDigestMerge() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // Create first t-digest with low values
      String key1 = k() + "1";
      redis.dispatch(
            command("TDIGEST.CREATE"),
            new StatusOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(key1));
      redis.dispatch(
            command("TDIGEST.ADD"),
            new StatusOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(key1).add("1").add("2").add("3"));

      // Create second t-digest with high values
      String key2 = k() + "2";
      redis.dispatch(
            command("TDIGEST.CREATE"),
            new StatusOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(key2));
      redis.dispatch(
            command("TDIGEST.ADD"),
            new StatusOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(key2).add("8").add("9").add("10"));

      // Merge into destination
      String destKey = k() + "dest";
      String result = redis.dispatch(
            command("TDIGEST.MERGE"),
            new StatusOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(destKey).add("2").addKey(key1).addKey(key2));
      assertThat(result).isEqualTo("OK");

      // Verify merged min/max
      List<Object> minResult = redis.dispatch(
            command("TDIGEST.MIN"),
            new ArrayOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(destKey));
      assertThat(Double.parseDouble(minResult.get(0).toString())).isCloseTo(1.0, within(0.1));

      List<Object> maxResult = redis.dispatch(
            command("TDIGEST.MAX"),
            new ArrayOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(destKey));
      assertThat(Double.parseDouble(maxResult.get(0).toString())).isCloseTo(10.0, within(0.1));
   }

   @Test
   public void testTDigestCreateInvalidCompression() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // compression = 0 should fail
      assertThatThrownBy(() -> redis.dispatch(
            command("TDIGEST.CREATE"),
            new StatusOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("COMPRESSION").add("0")))
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("compression must be positive");

      // compression < 0 should fail
      assertThatThrownBy(() -> redis.dispatch(
            command("TDIGEST.CREATE"),
            new StatusOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k() + "2").add("COMPRESSION").add("-100")))
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("compression must be positive");
   }

   @Test
   public void testTDigestMinMaxEmpty() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // Create empty t-digest
      redis.dispatch(
            command("TDIGEST.CREATE"),
            new StatusOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()));

      // Min of empty should return nan
      List<Object> minResult = redis.dispatch(
            command("TDIGEST.MIN"),
            new ArrayOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()));
      assertThat(minResult).hasSize(1);
      double minValue = Double.parseDouble(minResult.get(0).toString());
      assertThat(Double.isNaN(minValue)).isTrue();

      // Max of empty should return nan
      List<Object> maxResult = redis.dispatch(
            command("TDIGEST.MAX"),
            new ArrayOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()));
      assertThat(maxResult).hasSize(1);
      double maxValue = Double.parseDouble(maxResult.get(0).toString());
      assertThat(Double.isNaN(maxValue)).isTrue();
   }

   @Test
   public void testTDigestTrimmedMeanInvalidRange() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // Create and add values
      redis.dispatch(
            command("TDIGEST.CREATE"),
            new StatusOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()));
      redis.dispatch(
            command("TDIGEST.ADD"),
            new StatusOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("1").add("2").add("3"));

      // lowFraction < 0 should fail
      assertThatThrownBy(() -> redis.dispatch(
            command("TDIGEST.TRIMMED_MEAN"),
            new ArrayOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("-0.1").add("0.9")))
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("must be between 0 and 1");

      // highFraction > 1 should fail
      assertThatThrownBy(() -> redis.dispatch(
            command("TDIGEST.TRIMMED_MEAN"),
            new ArrayOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("0.1").add("1.5")))
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("must be between 0 and 1");
   }

   @Test
   public void testTDigestMergeWithCompression() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // Create source t-digests
      String key1 = k() + "1";
      redis.dispatch(
            command("TDIGEST.CREATE"),
            new StatusOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(key1).add("COMPRESSION").add("100"));
      redis.dispatch(
            command("TDIGEST.ADD"),
            new StatusOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(key1).add("1").add("2").add("3"));

      String key2 = k() + "2";
      redis.dispatch(
            command("TDIGEST.CREATE"),
            new StatusOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(key2).add("COMPRESSION").add("100"));
      redis.dispatch(
            command("TDIGEST.ADD"),
            new StatusOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(key2).add("4").add("5").add("6"));

      // Merge with custom compression
      String destKey = k() + "dest";
      String result = redis.dispatch(
            command("TDIGEST.MERGE"),
            new StatusOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(destKey).add("2").addKey(key1).addKey(key2)
                  .add("COMPRESSION").add("200"));
      assertThat(result).isEqualTo("OK");

      // Verify compression in info
      List<Object> info = redis.dispatch(
            command("TDIGEST.INFO"),
            new ArrayOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(destKey));
      assertThat(info.get(1)).isEqualTo(200L);
   }

   @Test
   public void testTDigestResetNonExistent() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // Reset on non-existent key should fail
      assertThatThrownBy(() -> redis.dispatch(
            command("TDIGEST.RESET"),
            new StatusOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k())))
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("not found");
   }

   @Test
   public void testTDigestWrongType() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // Create a string key
      redis.set(k(), "value");

      // TDIGEST.ADD on wrong type should fail
      assertThatThrownBy(() -> redis.dispatch(
            command("TDIGEST.ADD"),
            new StatusOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("1")))
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("WRONGTYPE");

      // TDIGEST.INFO on wrong type should fail
      assertThatThrownBy(() -> redis.dispatch(
            command("TDIGEST.INFO"),
            new ArrayOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k())))
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("WRONGTYPE");

      // TDIGEST.MIN on wrong type should fail
      assertThatThrownBy(() -> redis.dispatch(
            command("TDIGEST.MIN"),
            new ArrayOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k())))
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("WRONGTYPE");

      // TDIGEST.QUANTILE on wrong type should fail
      assertThatThrownBy(() -> redis.dispatch(
            command("TDIGEST.QUANTILE"),
            new ArrayOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("0.5")))
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("WRONGTYPE");
   }

   @Test
   public void testTDigestQuantileMultiple() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // Create and add values (1-100)
      redis.dispatch(
            command("TDIGEST.CREATE"),
            new StatusOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()));

      CommandArgs<String, String> addArgs = new CommandArgs<>(StringCodec.UTF8).addKey(k());
      for (int i = 1; i <= 100; i++) {
         addArgs.add(String.valueOf(i));
      }
      redis.dispatch(command("TDIGEST.ADD"), new StatusOutput<>(StringCodec.UTF8), addArgs);

      // Get multiple quantiles in one call
      List<Object> result = redis.dispatch(
            command("TDIGEST.QUANTILE"),
            new ArrayOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("0.25").add("0.5").add("0.75"));

      assertThat(result).hasSize(3);
      double q25 = Double.parseDouble(result.get(0).toString());
      double q50 = Double.parseDouble(result.get(1).toString());
      double q75 = Double.parseDouble(result.get(2).toString());

      // 25th percentile should be around 25
      assertThat(q25).isCloseTo(25.0, within(10.0));
      // 50th percentile (median) should be around 50
      assertThat(q50).isCloseTo(50.0, within(10.0));
      // 75th percentile should be around 75
      assertThat(q75).isCloseTo(75.0, within(10.0));
   }

   @Test
   public void testTDigestCdfMultiple() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // Create and add values (1-100)
      redis.dispatch(
            command("TDIGEST.CREATE"),
            new StatusOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()));

      CommandArgs<String, String> addArgs = new CommandArgs<>(StringCodec.UTF8).addKey(k());
      for (int i = 1; i <= 100; i++) {
         addArgs.add(String.valueOf(i));
      }
      redis.dispatch(command("TDIGEST.ADD"), new StatusOutput<>(StringCodec.UTF8), addArgs);

      // Get CDF for multiple values
      List<Object> result = redis.dispatch(
            command("TDIGEST.CDF"),
            new ArrayOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("25").add("50").add("75"));

      assertThat(result).hasSize(3);
      double cdf25 = Double.parseDouble(result.get(0).toString());
      double cdf50 = Double.parseDouble(result.get(1).toString());
      double cdf75 = Double.parseDouble(result.get(2).toString());

      // CDF of 25 should be around 0.25
      assertThat(cdf25).isCloseTo(0.25, within(0.1));
      // CDF of 50 should be around 0.5
      assertThat(cdf50).isCloseTo(0.5, within(0.1));
      // CDF of 75 should be around 0.75
      assertThat(cdf75).isCloseTo(0.75, within(0.1));
   }

   @Test
   public void testTDigestRankMultiple() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // Create and add values
      redis.dispatch(
            command("TDIGEST.CREATE"),
            new StatusOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()));
      redis.dispatch(
            command("TDIGEST.ADD"),
            new StatusOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("10").add("20").add("30").add("40").add("50"));

      // Get ranks for multiple values
      List<Object> result = redis.dispatch(
            command("TDIGEST.RANK"),
            new ArrayOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("10").add("30").add("50"));

      assertThat(result).hasSize(3);
   }

   @Test
   public void testTDigestByRankMultiple() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // Create and add values
      redis.dispatch(
            command("TDIGEST.CREATE"),
            new StatusOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()));
      redis.dispatch(
            command("TDIGEST.ADD"),
            new StatusOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("10").add("20").add("30").add("40").add("50"));

      // Get values at multiple ranks
      List<Object> result = redis.dispatch(
            command("TDIGEST.BYRANK"),
            new ArrayOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("0").add("2").add("4"));

      assertThat(result).hasSize(3);
      // Rank 0 should be min (10)
      double r0 = Double.parseDouble(result.get(0).toString());
      assertThat(r0).isCloseTo(10.0, within(5.0));
   }

   @Test
   public void testTDigestMinNonExistent() {
      RedisCommands<String, String> redis = redisConnection.sync();

      assertThatThrownBy(() -> redis.dispatch(
            command("TDIGEST.MIN"),
            new ArrayOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k())))
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("not found");
   }

   @Test
   public void testTDigestMaxNonExistent() {
      RedisCommands<String, String> redis = redisConnection.sync();

      assertThatThrownBy(() -> redis.dispatch(
            command("TDIGEST.MAX"),
            new ArrayOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k())))
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("not found");
   }

   @Test
   public void testTDigestQuantileNonExistent() {
      RedisCommands<String, String> redis = redisConnection.sync();

      assertThatThrownBy(() -> redis.dispatch(
            command("TDIGEST.QUANTILE"),
            new ArrayOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("0.5")))
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("not found");
   }

   @Test
   public void testTDigestCdfNonExistent() {
      RedisCommands<String, String> redis = redisConnection.sync();

      assertThatThrownBy(() -> redis.dispatch(
            command("TDIGEST.CDF"),
            new ArrayOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("50")))
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("not found");
   }

   @Test
   public void testTDigestMergeNonExistentSource() {
      RedisCommands<String, String> redis = redisConnection.sync();

      String destKey = k() + "dest";
      String srcKey = k() + "src";

      assertThatThrownBy(() -> redis.dispatch(
            command("TDIGEST.MERGE"),
            new StatusOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(destKey).add("1").addKey(srcKey)))
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("does not exist");
   }

   private SimpleCommand command(String name) {
      return new SimpleCommand(name);
   }

   private static class SimpleCommand implements io.lettuce.core.protocol.ProtocolKeyword {
      private final String name;

      SimpleCommand(String name) {
         this.name = name;
      }

      @Override
      public byte[] getBytes() {
         return name.getBytes();
      }

      @Override
      public String name() {
         return name;
      }
   }
}
