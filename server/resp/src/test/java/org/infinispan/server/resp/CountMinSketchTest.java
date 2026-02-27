package org.infinispan.server.resp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.infinispan.test.TestingUtil.k;

import java.util.List;

import org.testng.annotations.Test;

import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.output.ArrayOutput;
import io.lettuce.core.output.StatusOutput;
import io.lettuce.core.protocol.CommandArgs;

@Test(groups = "functional", testName = "server.resp.CountMinSketchTest")
public class CountMinSketchTest extends SingleNodeRespBaseTest {

   @Test
   public void testCmsInitByDim() {
      RedisCommands<String, String> redis = redisConnection.sync();

      String result = redis.dispatch(
            command("CMS.INITBYDIM"),
            new StatusOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("1000").add("5"));
      assertThat(result).isEqualTo("OK");

      // Try to initialize the same key again - should fail
      assertThatThrownBy(() -> redis.dispatch(
            command("CMS.INITBYDIM"),
            new StatusOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("1000").add("5")))
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("item exists");
   }

   @Test
   public void testCmsInitByProb() {
      RedisCommands<String, String> redis = redisConnection.sync();

      String result = redis.dispatch(
            command("CMS.INITBYPROB"),
            new StatusOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("0.001").add("0.01"));
      assertThat(result).isEqualTo("OK");

      // Try to initialize the same key again - should fail
      assertThatThrownBy(() -> redis.dispatch(
            command("CMS.INITBYPROB"),
            new StatusOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("0.001").add("0.01")))
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("item exists");
   }

   @Test
   public void testCmsIncrBy() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // Create sketch
      redis.dispatch(
            command("CMS.INITBYDIM"),
            new StatusOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("1000").add("5"));

      // Increment items
      List<Object> results = redis.dispatch(
            command("CMS.INCRBY"),
            new ArrayOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("item1").add("5").add("item2").add("3"));
      assertThat(results).containsExactly(5L, 3L);

      // Increment again
      results = redis.dispatch(
            command("CMS.INCRBY"),
            new ArrayOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("item1").add("2"));
      assertThat(results).containsExactly(7L);
   }

   @Test
   public void testCmsIncrByOnNonExistent() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // Try to increment on non-existent key
      assertThatThrownBy(() -> redis.dispatch(
            command("CMS.INCRBY"),
            new ArrayOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("item1").add("5")))
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("not found");
   }

   @Test
   public void testCmsQuery() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // Create sketch and add items
      redis.dispatch(
            command("CMS.INITBYDIM"),
            new StatusOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("1000").add("5"));

      redis.dispatch(
            command("CMS.INCRBY"),
            new ArrayOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("item1").add("10").add("item2").add("20"));

      // Query items
      List<Object> results = redis.dispatch(
            command("CMS.QUERY"),
            new ArrayOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("item1").add("item2").add("item3"));

      // item1 should be at least 10, item2 at least 20, item3 may have some false positives
      assertThat((Long) results.get(0)).isGreaterThanOrEqualTo(10L);
      assertThat((Long) results.get(1)).isGreaterThanOrEqualTo(20L);
   }

   @Test
   public void testCmsQueryNonExistent() {
      RedisCommands<String, String> redis = redisConnection.sync();

      assertThatThrownBy(() -> redis.dispatch(
            command("CMS.QUERY"),
            new ArrayOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("item1")))
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("not found");
   }

   @Test
   public void testCmsInfo() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // Create sketch
      redis.dispatch(
            command("CMS.INITBYDIM"),
            new StatusOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("1000").add("5"));

      // Add some items
      redis.dispatch(
            command("CMS.INCRBY"),
            new ArrayOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("item1").add("10").add("item2").add("20"));

      // Get info
      List<Object> info = redis.dispatch(
            command("CMS.INFO"),
            new ArrayOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()));

      // Should have 6 elements (3 key-value pairs: width, depth, count)
      assertThat(info).hasSize(6);
      assertThat(info.get(0)).isEqualTo("width");
      assertThat(info.get(1)).isEqualTo(1000L);
      assertThat(info.get(2)).isEqualTo("depth");
      assertThat(info.get(3)).isEqualTo(5L);
      assertThat(info.get(4)).isEqualTo("count");
      assertThat(info.get(5)).isEqualTo(30L); // 10 + 20 = 30
   }

   @Test
   public void testCmsInfoNonExistent() {
      RedisCommands<String, String> redis = redisConnection.sync();

      assertThatThrownBy(() -> redis.dispatch(
            command("CMS.INFO"),
            new ArrayOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k())))
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("not found");
   }

   @Test
   public void testCmsMerge() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // Create first sketch
      String key1 = k() + "1";
      redis.dispatch(
            command("CMS.INITBYDIM"),
            new StatusOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(key1).add("1000").add("5"));
      redis.dispatch(
            command("CMS.INCRBY"),
            new ArrayOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(key1).add("item1").add("10"));

      // Create second sketch
      String key2 = k() + "2";
      redis.dispatch(
            command("CMS.INITBYDIM"),
            new StatusOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(key2).add("1000").add("5"));
      redis.dispatch(
            command("CMS.INCRBY"),
            new ArrayOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(key2).add("item1").add("20"));

      // Merge into destination
      String destKey = k() + "dest";
      String result = redis.dispatch(
            command("CMS.MERGE"),
            new StatusOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(destKey).add("2").addKey(key1).addKey(key2));
      assertThat(result).isEqualTo("OK");

      // Query merged sketch
      List<Object> queryResult = redis.dispatch(
            command("CMS.QUERY"),
            new ArrayOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(destKey).add("item1"));
      assertThat((Long) queryResult.get(0)).isGreaterThanOrEqualTo(30L); // 10 + 20
   }

   @Test
   public void testCmsMergeWithWeights() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // Create first sketch
      String key1 = k() + "1";
      redis.dispatch(
            command("CMS.INITBYDIM"),
            new StatusOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(key1).add("1000").add("5"));
      redis.dispatch(
            command("CMS.INCRBY"),
            new ArrayOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(key1).add("item1").add("10"));

      // Create second sketch
      String key2 = k() + "2";
      redis.dispatch(
            command("CMS.INITBYDIM"),
            new StatusOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(key2).add("1000").add("5"));
      redis.dispatch(
            command("CMS.INCRBY"),
            new ArrayOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(key2).add("item1").add("20"));

      // Merge with weights
      String destKey = k() + "dest";
      String result = redis.dispatch(
            command("CMS.MERGE"),
            new StatusOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(destKey).add("2").addKey(key1).addKey(key2)
                  .add("WEIGHTS").add("2").add("3"));
      assertThat(result).isEqualTo("OK");

      // Query merged sketch - should be (10*2 + 20*3) = 80
      List<Object> queryResult = redis.dispatch(
            command("CMS.QUERY"),
            new ArrayOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(destKey).add("item1"));
      assertThat((Long) queryResult.get(0)).isGreaterThanOrEqualTo(80L);
   }

   @Test
   public void testCmsMergeNonExistentSource() {
      RedisCommands<String, String> redis = redisConnection.sync();

      String destKey = k() + "dest";
      String srcKey = k() + "src";

      assertThatThrownBy(() -> redis.dispatch(
            command("CMS.MERGE"),
            new StatusOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(destKey).add("1").addKey(srcKey)))
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("does not exist");
   }

   @Test
   public void testCmsInitByDimInvalidWidth() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // width = 0 should fail
      assertThatThrownBy(() -> redis.dispatch(
            command("CMS.INITBYDIM"),
            new StatusOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("0").add("5")))
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("must be positive");

      // width < 0 should fail
      assertThatThrownBy(() -> redis.dispatch(
            command("CMS.INITBYDIM"),
            new StatusOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k() + "2").add("-10").add("5")))
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("must be positive");
   }

   @Test
   public void testCmsInitByDimInvalidDepth() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // depth = 0 should fail
      assertThatThrownBy(() -> redis.dispatch(
            command("CMS.INITBYDIM"),
            new StatusOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("1000").add("0")))
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("must be positive");

      // depth < 0 should fail
      assertThatThrownBy(() -> redis.dispatch(
            command("CMS.INITBYDIM"),
            new StatusOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k() + "2").add("1000").add("-5")))
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("must be positive");
   }

   @Test
   public void testCmsInitByProbInvalidError() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // error = 0 should fail
      assertThatThrownBy(() -> redis.dispatch(
            command("CMS.INITBYPROB"),
            new StatusOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("0").add("0.01")))
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("must be between 0 and 1");

      // error = 1 should fail
      assertThatThrownBy(() -> redis.dispatch(
            command("CMS.INITBYPROB"),
            new StatusOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k() + "2").add("1").add("0.01")))
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("must be between 0 and 1");

      // error > 1 should fail
      assertThatThrownBy(() -> redis.dispatch(
            command("CMS.INITBYPROB"),
            new StatusOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k() + "3").add("1.5").add("0.01")))
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("must be between 0 and 1");
   }

   @Test
   public void testCmsInitByProbInvalidProbability() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // probability = 0 should fail
      assertThatThrownBy(() -> redis.dispatch(
            command("CMS.INITBYPROB"),
            new StatusOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("0.001").add("0")))
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("must be between 0 and 1");

      // probability = 1 should fail
      assertThatThrownBy(() -> redis.dispatch(
            command("CMS.INITBYPROB"),
            new StatusOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k() + "2").add("0.001").add("1")))
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("must be between 0 and 1");

      // probability > 1 should fail
      assertThatThrownBy(() -> redis.dispatch(
            command("CMS.INITBYPROB"),
            new StatusOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k() + "3").add("0.001").add("2.0")))
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("must be between 0 and 1");
   }

   @Test
   public void testCmsMergeMismatchedDimensions() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // Create first sketch with 1000x5
      String key1 = k() + "1";
      redis.dispatch(
            command("CMS.INITBYDIM"),
            new StatusOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(key1).add("1000").add("5"));

      // Create second sketch with different dimensions 500x3
      String key2 = k() + "2";
      redis.dispatch(
            command("CMS.INITBYDIM"),
            new StatusOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(key2).add("500").add("3"));

      // Merge should fail due to dimension mismatch
      String destKey = k() + "dest";
      assertThatThrownBy(() -> redis.dispatch(
            command("CMS.MERGE"),
            new StatusOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(destKey).add("2").addKey(key1).addKey(key2)))
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("width/depth doesn't match");
   }

   @Test
   public void testCmsWrongType() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // Create a string key
      redis.set(k(), "value");

      // CMS.INCRBY on wrong type should fail
      assertThatThrownBy(() -> redis.dispatch(
            command("CMS.INCRBY"),
            new ArrayOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("item1").add("5")))
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("WRONGTYPE");

      // CMS.QUERY on wrong type should fail
      assertThatThrownBy(() -> redis.dispatch(
            command("CMS.QUERY"),
            new ArrayOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("item1")))
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("WRONGTYPE");

      // CMS.INFO on wrong type should fail
      assertThatThrownBy(() -> redis.dispatch(
            command("CMS.INFO"),
            new ArrayOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k())))
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("WRONGTYPE");
   }

   @Test
   public void testCmsEmptyString() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // Create sketch
      redis.dispatch(
            command("CMS.INITBYDIM"),
            new StatusOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("1000").add("5"));

      // Increment empty string
      List<Object> results = redis.dispatch(
            command("CMS.INCRBY"),
            new ArrayOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("").add("10"));
      assertThat(results).containsExactly(10L);

      // Query empty string
      results = redis.dispatch(
            command("CMS.QUERY"),
            new ArrayOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add(""));
      assertThat((Long) results.get(0)).isGreaterThanOrEqualTo(10L);
   }

   @Test
   public void testCmsSmallDimensions() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // Create sketch with minimal dimensions (2x2)
      String result = redis.dispatch(
            command("CMS.INITBYDIM"),
            new StatusOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("2").add("2"));
      assertThat(result).isEqualTo("OK");

      // Add items
      List<Object> results = redis.dispatch(
            command("CMS.INCRBY"),
            new ArrayOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("item1").add("5").add("item2").add("3"));
      assertThat(results).containsExactly(5L, 3L);

      // Query - with small dimensions, we may have collisions but counts should be >= actual
      results = redis.dispatch(
            command("CMS.QUERY"),
            new ArrayOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("item1").add("item2"));
      assertThat((Long) results.get(0)).isGreaterThanOrEqualTo(5L);
      assertThat((Long) results.get(1)).isGreaterThanOrEqualTo(3L);

      // Verify info shows correct dimensions
      List<Object> info = redis.dispatch(
            command("CMS.INFO"),
            new ArrayOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()));
      assertThat(info.get(1)).isEqualTo(2L); // width
      assertThat(info.get(3)).isEqualTo(2L); // depth
   }

   @Test
   public void testCmsMergeNegativeWeights() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // Create first sketch
      String key1 = k() + "1";
      redis.dispatch(
            command("CMS.INITBYDIM"),
            new StatusOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(key1).add("1000").add("5"));
      redis.dispatch(
            command("CMS.INCRBY"),
            new ArrayOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(key1).add("item1").add("100"));

      // Create second sketch
      String key2 = k() + "2";
      redis.dispatch(
            command("CMS.INITBYDIM"),
            new StatusOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(key2).add("1000").add("5"));
      redis.dispatch(
            command("CMS.INCRBY"),
            new ArrayOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(key2).add("item1").add("30"));

      // Merge with negative weight for second sketch (subtraction)
      String destKey = k() + "dest";
      String result = redis.dispatch(
            command("CMS.MERGE"),
            new StatusOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(destKey).add("2").addKey(key1).addKey(key2)
                  .add("WEIGHTS").add("1").add("-1"));
      assertThat(result).isEqualTo("OK");

      // Query merged sketch - should be 100 - 30 = 70
      List<Object> queryResult = redis.dispatch(
            command("CMS.QUERY"),
            new ArrayOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(destKey).add("item1"));
      assertThat((Long) queryResult.get(0)).isGreaterThanOrEqualTo(70L);
   }

   @Test
   public void testCmsMultipleItems() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // Create sketch
      redis.dispatch(
            command("CMS.INITBYDIM"),
            new StatusOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("1000").add("5"));

      // Add many items in one call
      List<Object> results = redis.dispatch(
            command("CMS.INCRBY"),
            new ArrayOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k())
                  .add("a").add("1")
                  .add("b").add("2")
                  .add("c").add("3")
                  .add("d").add("4")
                  .add("e").add("5"));
      assertThat(results).containsExactly(1L, 2L, 3L, 4L, 5L);

      // Query all items
      results = redis.dispatch(
            command("CMS.QUERY"),
            new ArrayOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k())
                  .add("a").add("b").add("c").add("d").add("e"));
      assertThat((Long) results.get(0)).isGreaterThanOrEqualTo(1L);
      assertThat((Long) results.get(1)).isGreaterThanOrEqualTo(2L);
      assertThat((Long) results.get(2)).isGreaterThanOrEqualTo(3L);
      assertThat((Long) results.get(3)).isGreaterThanOrEqualTo(4L);
      assertThat((Long) results.get(4)).isGreaterThanOrEqualTo(5L);
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
