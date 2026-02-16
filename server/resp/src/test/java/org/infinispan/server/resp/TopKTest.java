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

@Test(groups = "functional", testName = "server.resp.TopKTest")
public class TopKTest extends SingleNodeRespBaseTest {

   @Test
   public void testTopKReserve() {
      RedisCommands<String, String> redis = redisConnection.sync();

      String result = redis.dispatch(
            command("TOPK.RESERVE"),
            new StatusOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("3"));
      assertThat(result).isEqualTo("OK");

      // Try to reserve the same key again - should fail
      assertThatThrownBy(() -> redis.dispatch(
            command("TOPK.RESERVE"),
            new StatusOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("3")))
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("item exists");
   }

   @Test
   public void testTopKReserveWithOptions() {
      RedisCommands<String, String> redis = redisConnection.sync();

      String result = redis.dispatch(
            command("TOPK.RESERVE"),
            new StatusOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("5").add("10").add("8").add("0.95"));
      assertThat(result).isEqualTo("OK");
   }

   @Test
   public void testTopKAdd() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // Create filter
      redis.dispatch(
            command("TOPK.RESERVE"),
            new StatusOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("3"));

      // Add items
      List<Object> results = redis.dispatch(
            command("TOPK.ADD"),
            new ArrayOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("item1").add("item2").add("item3"));
      assertThat(results).hasSize(3);
      // First 3 items should not expel anything (we have k=3)
      assertThat(results).containsOnly((Object) null);
   }

   @Test
   public void testTopKAddExpelling() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // Create filter with k=2
      redis.dispatch(
            command("TOPK.RESERVE"),
            new StatusOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("2"));

      // Add 2 items
      redis.dispatch(
            command("TOPK.ADD"),
            new ArrayOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("item1").add("item2"));

      // Add more items with increments to force expelling
      redis.dispatch(
            command("TOPK.INCRBY"),
            new ArrayOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("item1").add("10"));

      redis.dispatch(
            command("TOPK.INCRBY"),
            new ArrayOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("item2").add("5"));

      // Adding a new item with high count should expel one
      List<Object> results = redis.dispatch(
            command("TOPK.INCRBY"),
            new ArrayOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("item3").add("20"));
      // item3 should expel item2 (lower count)
      assertThat(results).hasSize(1);
   }

   @Test
   public void testTopKAddOnNonExistent() {
      RedisCommands<String, String> redis = redisConnection.sync();

      assertThatThrownBy(() -> redis.dispatch(
            command("TOPK.ADD"),
            new ArrayOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("item1")))
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("not found");
   }

   @Test
   public void testTopKIncrBy() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // Create filter
      redis.dispatch(
            command("TOPK.RESERVE"),
            new StatusOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("3"));

      // Add items
      redis.dispatch(
            command("TOPK.ADD"),
            new ArrayOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("item1").add("item2"));

      // Increment
      List<Object> results = redis.dispatch(
            command("TOPK.INCRBY"),
            new ArrayOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("item1").add("5").add("item2").add("3"));
      assertThat(results).hasSize(2);
   }

   @Test
   public void testTopKQuery() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // Create filter
      redis.dispatch(
            command("TOPK.RESERVE"),
            new StatusOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("3"));

      // Add items
      redis.dispatch(
            command("TOPK.ADD"),
            new ArrayOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("item1").add("item2"));

      // Query
      List<Object> results = redis.dispatch(
            command("TOPK.QUERY"),
            new ArrayOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("item1").add("item2").add("item3"));
      assertThat(results).containsExactly(1L, 1L, 0L);
   }

   @Test
   public void testTopKQueryNonExistent() {
      RedisCommands<String, String> redis = redisConnection.sync();

      assertThatThrownBy(() -> redis.dispatch(
            command("TOPK.QUERY"),
            new ArrayOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("item1")))
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("not found");
   }

   @Test
   public void testTopKList() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // Create filter
      redis.dispatch(
            command("TOPK.RESERVE"),
            new StatusOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("3"));

      // Add items with different counts
      redis.dispatch(
            command("TOPK.INCRBY"),
            new ArrayOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k())
                  .add("item1").add("10")
                  .add("item2").add("20")
                  .add("item3").add("5"));

      // List without counts - should be ordered by count descending
      List<Object> results = redis.dispatch(
            command("TOPK.LIST"),
            new ArrayOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()));
      assertThat(results).containsExactly("item2", "item1", "item3");
   }

   @Test
   public void testTopKListWithCount() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // Create filter
      redis.dispatch(
            command("TOPK.RESERVE"),
            new StatusOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("3"));

      // Add items with different counts
      redis.dispatch(
            command("TOPK.INCRBY"),
            new ArrayOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k())
                  .add("item1").add("10")
                  .add("item2").add("20"));

      // List with counts
      List<Object> results = redis.dispatch(
            command("TOPK.LIST"),
            new ArrayOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("WITHCOUNT"));
      // Should be: item2, 20, item1, 10
      assertThat(results).hasSize(4);
      assertThat(results.get(0)).isEqualTo("item2");
      assertThat(results.get(1)).isEqualTo(20L);
      assertThat(results.get(2)).isEqualTo("item1");
      assertThat(results.get(3)).isEqualTo(10L);
   }

   @Test
   public void testTopKInfo() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // Create filter with specific params
      redis.dispatch(
            command("TOPK.RESERVE"),
            new StatusOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("5").add("10").add("8").add("0.95"));

      // Get info
      List<Object> info = redis.dispatch(
            command("TOPK.INFO"),
            new ArrayOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()));

      // Should have 8 elements (4 key-value pairs: k, width, depth, decay)
      assertThat(info).hasSize(8);
      assertThat(info.get(0)).isEqualTo("k");
      assertThat(info.get(1)).isEqualTo(5L);
      assertThat(info.get(2)).isEqualTo("width");
      assertThat(info.get(3)).isEqualTo(10L);
      assertThat(info.get(4)).isEqualTo("depth");
      assertThat(info.get(5)).isEqualTo(8L);
      assertThat(info.get(6)).isEqualTo("decay");
   }

   @Test
   public void testTopKInfoNonExistent() {
      RedisCommands<String, String> redis = redisConnection.sync();

      assertThatThrownBy(() -> redis.dispatch(
            command("TOPK.INFO"),
            new ArrayOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k())))
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("not found");
   }

   @Test
   public void testTopKCount() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // Create filter
      redis.dispatch(
            command("TOPK.RESERVE"),
            new StatusOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("3"));

      // Add items with different counts
      redis.dispatch(
            command("TOPK.INCRBY"),
            new ArrayOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k())
                  .add("item1").add("10")
                  .add("item2").add("20"));

      // Get counts
      List<Object> results = redis.dispatch(
            command("TOPK.COUNT"),
            new ArrayOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("item1").add("item2").add("item3"));

      assertThat(results).hasSize(3);
      assertThat((Long) results.get(0)).isGreaterThanOrEqualTo(10L);
      assertThat((Long) results.get(1)).isGreaterThanOrEqualTo(20L);
   }

   @Test
   public void testTopKReserveInvalidK() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // k = 0 should fail
      assertThatThrownBy(() -> redis.dispatch(
            command("TOPK.RESERVE"),
            new StatusOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("0")))
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("k must be positive");

      // k < 0 should fail
      assertThatThrownBy(() -> redis.dispatch(
            command("TOPK.RESERVE"),
            new StatusOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k() + "2").add("-1")))
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("k must be positive");
   }

   @Test
   public void testTopKReserveInvalidWidth() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // width = 0 should fail
      assertThatThrownBy(() -> redis.dispatch(
            command("TOPK.RESERVE"),
            new StatusOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("3").add("0").add("5").add("0.9")))
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("width must be positive");

      // width < 0 should fail
      assertThatThrownBy(() -> redis.dispatch(
            command("TOPK.RESERVE"),
            new StatusOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k() + "2").add("3").add("-1").add("5").add("0.9")))
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("width must be positive");
   }

   @Test
   public void testTopKReserveInvalidDepth() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // depth = 0 should fail
      assertThatThrownBy(() -> redis.dispatch(
            command("TOPK.RESERVE"),
            new StatusOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("3").add("10").add("0").add("0.9")))
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("depth must be positive");

      // depth < 0 should fail
      assertThatThrownBy(() -> redis.dispatch(
            command("TOPK.RESERVE"),
            new StatusOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k() + "2").add("3").add("10").add("-1").add("0.9")))
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("depth must be positive");
   }

   @Test
   public void testTopKReserveInvalidDecay() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // decay > 1 should fail
      assertThatThrownBy(() -> redis.dispatch(
            command("TOPK.RESERVE"),
            new StatusOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("3").add("10").add("5").add("1.5")))
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("decay must be between");

      // decay = 0 should fail
      assertThatThrownBy(() -> redis.dispatch(
            command("TOPK.RESERVE"),
            new StatusOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k() + "2").add("3").add("10").add("5").add("0")))
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("decay must be between");

      // decay < 0 should fail
      assertThatThrownBy(() -> redis.dispatch(
            command("TOPK.RESERVE"),
            new StatusOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k() + "3").add("3").add("10").add("5").add("-0.5")))
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("decay must be between");
   }

   @Test
   public void testTopKIncrByNonExistent() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // INCRBY on non-existent key should fail
      assertThatThrownBy(() -> redis.dispatch(
            command("TOPK.INCRBY"),
            new ArrayOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("item1").add("5")))
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("not found");
   }

   @Test
   public void testTopKCountNonExistent() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // COUNT on non-existent key should fail
      assertThatThrownBy(() -> redis.dispatch(
            command("TOPK.COUNT"),
            new ArrayOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("item1")))
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("not found");
   }

   @Test
   public void testTopKListNonExistent() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // LIST on non-existent key should fail
      assertThatThrownBy(() -> redis.dispatch(
            command("TOPK.LIST"),
            new ArrayOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k())))
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("not found");
   }

   @Test
   public void testTopKEmptyString() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // Create filter
      redis.dispatch(
            command("TOPK.RESERVE"),
            new StatusOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("3"));

      // Add empty string
      List<Object> results = redis.dispatch(
            command("TOPK.ADD"),
            new ArrayOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add(""));
      assertThat(results).hasSize(1);

      // Query empty string
      results = redis.dispatch(
            command("TOPK.QUERY"),
            new ArrayOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add(""));
      assertThat(results).containsExactly(1L);

      // Count empty string
      results = redis.dispatch(
            command("TOPK.COUNT"),
            new ArrayOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add(""));
      assertThat(results).hasSize(1);
      assertThat((Long) results.get(0)).isGreaterThanOrEqualTo(1L);
   }

   @Test
   public void testTopKSpecialChars() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // Create filter
      redis.dispatch(
            command("TOPK.RESERVE"),
            new StatusOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("5"));

      // Add items with special characters (spaces)
      String itemWithSpaces = "item with spaces";

      List<Object> results = redis.dispatch(
            command("TOPK.ADD"),
            new ArrayOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add(itemWithSpaces));
      assertThat(results).hasSize(1);

      // Query these items
      results = redis.dispatch(
            command("TOPK.QUERY"),
            new ArrayOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add(itemWithSpaces));
      assertThat(results).containsExactly(1L);
   }

   @Test
   public void testTopKWrongType() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // Create a string key
      redis.set(k(), "value");

      // TOPK.ADD on wrong type should fail
      assertThatThrownBy(() -> redis.dispatch(
            command("TOPK.ADD"),
            new ArrayOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("item1")))
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("WRONGTYPE");

      // TOPK.QUERY on wrong type should fail
      assertThatThrownBy(() -> redis.dispatch(
            command("TOPK.QUERY"),
            new ArrayOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("item1")))
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("WRONGTYPE");

      // TOPK.INFO on wrong type should fail
      assertThatThrownBy(() -> redis.dispatch(
            command("TOPK.INFO"),
            new ArrayOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k())))
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("WRONGTYPE");

      // TOPK.LIST on wrong type should fail
      assertThatThrownBy(() -> redis.dispatch(
            command("TOPK.LIST"),
            new ArrayOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k())))
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("WRONGTYPE");

      // TOPK.COUNT on wrong type should fail
      assertThatThrownBy(() -> redis.dispatch(
            command("TOPK.COUNT"),
            new ArrayOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("item1")))
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("WRONGTYPE");

      // TOPK.INCRBY on wrong type should fail
      assertThatThrownBy(() -> redis.dispatch(
            command("TOPK.INCRBY"),
            new ArrayOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("item1").add("5")))
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("WRONGTYPE");
   }

   @Test
   public void testTopKReserveDefaults() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // Reserve with only k - should use default width/depth
      String result = redis.dispatch(
            command("TOPK.RESERVE"),
            new StatusOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("10"));
      assertThat(result).isEqualTo("OK");

      // Verify info shows default values
      List<Object> info = redis.dispatch(
            command("TOPK.INFO"),
            new ArrayOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()));

      assertThat(info).hasSize(8);
      assertThat(info.get(0)).isEqualTo("k");
      assertThat(info.get(1)).isEqualTo(10L);
      // width and depth should have been auto-calculated
      assertThat(info.get(2)).isEqualTo("width");
      assertThat((Long) info.get(3)).isGreaterThan(0L);
      assertThat(info.get(4)).isEqualTo("depth");
      assertThat((Long) info.get(5)).isGreaterThan(0L);
   }

   @Test
   public void testTopKListNoDuplicates() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // Create filter
      redis.dispatch(
            command("TOPK.RESERVE"),
            new StatusOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("5"));

      // Add same item multiple times
      for (int i = 0; i < 10; i++) {
         redis.dispatch(
               command("TOPK.ADD"),
               new ArrayOutput<>(StringCodec.UTF8),
               new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("item1"));
      }

      // Add other items
      redis.dispatch(
            command("TOPK.ADD"),
            new ArrayOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("item2").add("item3"));

      // List should not have duplicates
      List<Object> results = redis.dispatch(
            command("TOPK.LIST"),
            new ArrayOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()));

      // Count occurrences of each item
      long item1Count = results.stream().filter("item1"::equals).count();
      assertThat(item1Count).isEqualTo(1L);
   }

   @Test
   public void testTopKMultipleIncrBy() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // Create filter
      redis.dispatch(
            command("TOPK.RESERVE"),
            new StatusOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("3"));

      // Add items with multiple increments in one call
      List<Object> results = redis.dispatch(
            command("TOPK.INCRBY"),
            new ArrayOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k())
                  .add("a").add("3")
                  .add("b").add("2")
                  .add("c").add("1"));
      assertThat(results).hasSize(3);

      // Verify counts
      results = redis.dispatch(
            command("TOPK.COUNT"),
            new ArrayOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("a").add("b").add("c"));
      assertThat((Long) results.get(0)).isGreaterThanOrEqualTo(3L);
      assertThat((Long) results.get(1)).isGreaterThanOrEqualTo(2L);
      assertThat((Long) results.get(2)).isGreaterThanOrEqualTo(1L);
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
