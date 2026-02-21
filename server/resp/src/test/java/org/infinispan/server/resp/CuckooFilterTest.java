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
import io.lettuce.core.output.IntegerOutput;
import io.lettuce.core.output.StatusOutput;
import io.lettuce.core.protocol.CommandArgs;

@Test(groups = "functional", testName = "server.resp.CuckooFilterTest")
public class CuckooFilterTest extends SingleNodeRespBaseTest {

   @Test
   public void testCfReserve() {
      RedisCommands<String, String> redis = redisConnection.sync();

      String result = redis.dispatch(
            command("CF.RESERVE"),
            new StatusOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("1000"));
      assertThat(result).isEqualTo("OK");

      // Try to reserve the same key again - should fail
      assertThatThrownBy(() -> redis.dispatch(
            command("CF.RESERVE"),
            new StatusOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("1000")))
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("item exists");
   }

   @Test
   public void testCfReserveWithOptions() {
      RedisCommands<String, String> redis = redisConnection.sync();

      String result = redis.dispatch(
            command("CF.RESERVE"),
            new StatusOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("1000")
                  .add("BUCKETSIZE").add("4")
                  .add("MAXITERATIONS").add("50")
                  .add("EXPANSION").add("2"));
      assertThat(result).isEqualTo("OK");
   }

   @Test
   public void testCfAdd() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // Add an item - filter will be created automatically
      Long result = redis.dispatch(
            command("CF.ADD"),
            new IntegerOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("item1"));
      assertThat(result).isEqualTo(1L);

      // Add the same item again - should succeed (duplicates allowed)
      result = redis.dispatch(
            command("CF.ADD"),
            new IntegerOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("item1"));
      assertThat(result).isEqualTo(1L);
   }

   @Test
   public void testCfAddNx() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // Add an item
      Long result = redis.dispatch(
            command("CF.ADDNX"),
            new IntegerOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("item1"));
      assertThat(result).isEqualTo(1L);

      // Try to add the same item again - should return 0
      result = redis.dispatch(
            command("CF.ADDNX"),
            new IntegerOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("item1"));
      assertThat(result).isEqualTo(0L);

      // Add a different item - should return 1
      result = redis.dispatch(
            command("CF.ADDNX"),
            new IntegerOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("item2"));
      assertThat(result).isEqualTo(1L);
   }

   @Test
   public void testCfExists() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // Check non-existent key - should return 0
      Long result = redis.dispatch(
            command("CF.EXISTS"),
            new IntegerOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("item1"));
      assertThat(result).isEqualTo(0L);

      // Add item
      redis.dispatch(
            command("CF.ADD"),
            new IntegerOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("item1"));

      // Check existing item - should return 1
      result = redis.dispatch(
            command("CF.EXISTS"),
            new IntegerOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("item1"));
      assertThat(result).isEqualTo(1L);

      // Check non-existing item in existing filter - should return 0
      result = redis.dispatch(
            command("CF.EXISTS"),
            new IntegerOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("item2"));
      assertThat(result).isEqualTo(0L);
   }

   @Test
   public void testCfMexists() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // Add some items
      redis.dispatch(
            command("CF.ADD"),
            new IntegerOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("item1"));
      redis.dispatch(
            command("CF.ADD"),
            new IntegerOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("item2"));

      // Check multiple items
      List<Object> results = redis.dispatch(
            command("CF.MEXISTS"),
            new ArrayOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("item1").add("item2").add("item3"));
      assertThat(results).containsExactly(1L, 1L, 0L);
   }

   @Test
   public void testCfDel() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // Add item
      redis.dispatch(
            command("CF.ADD"),
            new IntegerOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("item1"));

      // Delete item - should return 1
      Long result = redis.dispatch(
            command("CF.DEL"),
            new IntegerOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("item1"));
      assertThat(result).isEqualTo(1L);

      // Delete same item again - should return 0
      result = redis.dispatch(
            command("CF.DEL"),
            new IntegerOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("item1"));
      assertThat(result).isEqualTo(0L);

      // Verify item no longer exists
      result = redis.dispatch(
            command("CF.EXISTS"),
            new IntegerOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("item1"));
      assertThat(result).isEqualTo(0L);
   }

   @Test
   public void testCfCount() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // Count on non-existent key - should return 0
      Long result = redis.dispatch(
            command("CF.COUNT"),
            new IntegerOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("item1"));
      assertThat(result).isEqualTo(0L);

      // Add item twice
      redis.dispatch(
            command("CF.ADD"),
            new IntegerOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("item1"));
      redis.dispatch(
            command("CF.ADD"),
            new IntegerOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("item1"));

      // Count should be 2
      result = redis.dispatch(
            command("CF.COUNT"),
            new IntegerOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("item1"));
      assertThat(result).isEqualTo(2L);

      // Delete one occurrence
      redis.dispatch(
            command("CF.DEL"),
            new IntegerOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("item1"));

      // Count should be 1
      result = redis.dispatch(
            command("CF.COUNT"),
            new IntegerOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("item1"));
      assertThat(result).isEqualTo(1L);
   }

   @Test
   public void testCfInsert() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // Insert with default options
      List<Object> results = redis.dispatch(
            command("CF.INSERT"),
            new ArrayOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("ITEMS").add("item1").add("item2"));
      assertThat(results).containsExactly(1L, 1L);

      // Insert same items again - duplicates allowed so should succeed
      results = redis.dispatch(
            command("CF.INSERT"),
            new ArrayOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("ITEMS").add("item1").add("item3"));
      assertThat(results).containsExactly(1L, 1L);
   }

   @Test
   public void testCfInsertWithCapacity() {
      RedisCommands<String, String> redis = redisConnection.sync();

      List<Object> results = redis.dispatch(
            command("CF.INSERT"),
            new ArrayOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("CAPACITY").add("500").add("ITEMS").add("item1"));
      assertThat(results).containsExactly(1L);
   }

   @Test
   public void testCfInsertNocreate() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // Insert with NOCREATE on non-existent key - should fail
      assertThatThrownBy(() -> redis.dispatch(
            command("CF.INSERT"),
            new ArrayOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("NOCREATE").add("ITEMS").add("item1")))
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("not found");

      // Create filter first
      redis.dispatch(
            command("CF.ADD"),
            new IntegerOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("item1"));

      // Now NOCREATE should work
      List<Object> results = redis.dispatch(
            command("CF.INSERT"),
            new ArrayOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("NOCREATE").add("ITEMS").add("item2"));
      assertThat(results).containsExactly(1L);
   }

   @Test
   public void testCfInsertNx() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // InsertNX items
      List<Object> results = redis.dispatch(
            command("CF.INSERTNX"),
            new ArrayOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("ITEMS").add("item1").add("item2"));
      assertThat(results).containsExactly(1L, 1L);

      // InsertNX same items - should return 0 for existing
      results = redis.dispatch(
            command("CF.INSERTNX"),
            new ArrayOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("ITEMS").add("item1").add("item3"));
      assertThat(results).containsExactly(0L, 1L);
   }

   @Test
   public void testCfInfo() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // Create filter with specific params
      redis.dispatch(
            command("CF.RESERVE"),
            new StatusOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("1000")
                  .add("BUCKETSIZE").add("4")
                  .add("EXPANSION").add("2"));

      // Add some items
      redis.dispatch(
            command("CF.ADD"),
            new IntegerOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("item1"));
      redis.dispatch(
            command("CF.ADD"),
            new IntegerOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("item2"));
      redis.dispatch(
            command("CF.ADD"),
            new IntegerOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("item3"));

      // Get info
      List<Object> info = redis.dispatch(
            command("CF.INFO"),
            new ArrayOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()));

      // Should have 16 elements (8 key-value pairs)
      assertThat(info).hasSize(16);
      assertThat(info.get(0)).isEqualTo("Size");
      assertThat(info.get(2)).isEqualTo("Number of buckets");
      assertThat(info.get(4)).isEqualTo("Number of filter");
      assertThat(info.get(6)).isEqualTo("Number of items inserted");
      assertThat(info.get(7)).isEqualTo(3L);
      assertThat(info.get(10)).isEqualTo("Bucket size");
      assertThat(info.get(11)).isEqualTo(4L);
      assertThat(info.get(12)).isEqualTo("Expansion rate");
      assertThat(info.get(13)).isEqualTo(2L);
   }

   @Test
   public void testCfInfoNonExistentKey() {
      RedisCommands<String, String> redis = redisConnection.sync();

      assertThatThrownBy(() -> redis.dispatch(
            command("CF.INFO"),
            new ArrayOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k())))
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("not found");
   }

   @Test
   public void testCfReserveInvalidParams() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // Invalid capacity (zero)
      assertThatThrownBy(() -> redis.dispatch(
            command("CF.RESERVE"),
            new StatusOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("0")))
            .isInstanceOf(RedisCommandExecutionException.class);

      // Invalid capacity (negative)
      assertThatThrownBy(() -> redis.dispatch(
            command("CF.RESERVE"),
            new StatusOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k() + "2").add("-100")))
            .isInstanceOf(RedisCommandExecutionException.class);
   }

   @Test
   public void testCfReserveInvalidBucketSize() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // Invalid bucket size (zero)
      assertThatThrownBy(() -> redis.dispatch(
            command("CF.RESERVE"),
            new StatusOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("1000").add("BUCKETSIZE").add("0")))
            .isInstanceOf(RedisCommandExecutionException.class);

      // Invalid bucket size (negative)
      assertThatThrownBy(() -> redis.dispatch(
            command("CF.RESERVE"),
            new StatusOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k() + "2").add("1000").add("BUCKETSIZE").add("-1")))
            .isInstanceOf(RedisCommandExecutionException.class);
   }

   @Test
   public void testCfReserveInvalidExpansion() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // Invalid expansion (negative)
      assertThatThrownBy(() -> redis.dispatch(
            command("CF.RESERVE"),
            new StatusOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("1000").add("EXPANSION").add("-1")))
            .isInstanceOf(RedisCommandExecutionException.class);
   }

   @Test
   public void testCfReserveInvalidMaxIterations() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // Invalid max iterations (zero)
      assertThatThrownBy(() -> redis.dispatch(
            command("CF.RESERVE"),
            new StatusOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("1000").add("MAXITERATIONS").add("0")))
            .isInstanceOf(RedisCommandExecutionException.class);

      // Invalid max iterations (negative)
      assertThatThrownBy(() -> redis.dispatch(
            command("CF.RESERVE"),
            new StatusOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k() + "2").add("1000").add("MAXITERATIONS").add("-1")))
            .isInstanceOf(RedisCommandExecutionException.class);
   }

   @Test
   public void testCfMexistsNonExistentKey() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // Check items on non-existent key - all should return 0
      List<Object> results = redis.dispatch(
            command("CF.MEXISTS"),
            new ArrayOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("item1").add("item2"));
      assertThat(results).containsExactly(0L, 0L);
   }

   @Test
   public void testCfExpansion() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // Create a filter with expansion enabled
      redis.dispatch(
            command("CF.RESERVE"),
            new StatusOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("100").add("EXPANSION").add("2"));

      // Add items - filter should be able to hold many items due to expansion
      int successCount = 0;
      for (int i = 0; i < 200; i++) {
         Long result = redis.dispatch(
               command("CF.ADD"),
               new IntegerOutput<>(StringCodec.UTF8),
               new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("item" + i));
         if (result == 1L) {
            successCount++;
         }
      }

      // With expansion enabled, should be able to add many items
      assertThat(successCount).isGreaterThan(100);

      // Verify via CF.INFO that items are tracked
      List<Object> info = redis.dispatch(
            command("CF.INFO"),
            new ArrayOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()));

      // Find "Number of items inserted" in info
      for (int i = 0; i < info.size() - 1; i++) {
         if ("Number of items inserted".equals(info.get(i))) {
            Long count = (Long) info.get(i + 1);
            assertThat(count).isGreaterThan(100L);
            break;
         }
      }
   }

   @Test
   public void testCfNoExpansion() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // Create a filter with expansion = 0 (no expansion)
      // With 16 buckets (rounded from 10) * 2 bucket size = 32 slots
      redis.dispatch(
            command("CF.RESERVE"),
            new StatusOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("10").add("EXPANSION").add("0"));

      // Add items up to and beyond capacity - should eventually return -1 (filter full)
      boolean gotFilterFull = false;
      for (int i = 0; i < 100; i++) {
         Long result = redis.dispatch(
               command("CF.ADD"),
               new IntegerOutput<>(StringCodec.UTF8),
               new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("item" + i));
         if (result == -1L) {
            gotFilterFull = true;
            break;
         }
      }
      assertThat(gotFilterFull).isTrue();
   }

   @Test
   public void testCfDelNonExistentKey() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // Delete from non-existent key - returns 0 (not found)
      Long result = redis.dispatch(
            command("CF.DEL"),
            new IntegerOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("item1"));
      assertThat(result).isEqualTo(0L);
   }

   @Test
   public void testCfWrongType() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // Create a string key
      redis.set(k(), "value");

      // CF.ADD on wrong type should fail
      assertThatThrownBy(() -> redis.dispatch(
            command("CF.ADD"),
            new IntegerOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("item1")))
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("WRONGTYPE");

      // CF.EXISTS on wrong type should fail
      assertThatThrownBy(() -> redis.dispatch(
            command("CF.EXISTS"),
            new IntegerOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("item1")))
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("WRONGTYPE");

      // CF.INFO on wrong type should fail
      assertThatThrownBy(() -> redis.dispatch(
            command("CF.INFO"),
            new ArrayOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k())))
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("WRONGTYPE");

      // CF.DEL on wrong type should fail
      assertThatThrownBy(() -> redis.dispatch(
            command("CF.DEL"),
            new IntegerOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("item1")))
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("WRONGTYPE");
   }

   @Test
   public void testCfBucketSizeBehavior() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // Create filter with specific bucket size
      redis.dispatch(
            command("CF.RESERVE"),
            new StatusOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("100").add("BUCKETSIZE").add("2"));

      // Add items
      for (int i = 0; i < 20; i++) {
         redis.dispatch(
               command("CF.ADD"),
               new IntegerOutput<>(StringCodec.UTF8),
               new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("item" + i));
      }

      // Verify bucket size in info
      List<Object> info = redis.dispatch(
            command("CF.INFO"),
            new ArrayOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()));

      for (int i = 0; i < info.size() - 1; i++) {
         if ("Bucket size".equals(info.get(i))) {
            assertThat(info.get(i + 1)).isEqualTo(2L);
            break;
         }
      }

      // Verify all items are found
      for (int i = 0; i < 20; i++) {
         Long result = redis.dispatch(
               command("CF.EXISTS"),
               new IntegerOutput<>(StringCodec.UTF8),
               new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("item" + i));
         assertThat(result).isEqualTo(1L);
      }
   }

   @Test
   public void testCfCountNonExistentKey() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // Count on completely non-existent key should return 0
      Long result = redis.dispatch(
            command("CF.COUNT"),
            new IntegerOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("item1"));
      assertThat(result).isEqualTo(0L);
   }

   @Test
   public void testCfAddNxVsAdd() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // Add same item multiple times with CF.ADD - count should increase
      redis.dispatch(
            command("CF.ADD"),
            new IntegerOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("item1"));
      redis.dispatch(
            command("CF.ADD"),
            new IntegerOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("item1"));
      redis.dispatch(
            command("CF.ADD"),
            new IntegerOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("item1"));

      Long count = redis.dispatch(
            command("CF.COUNT"),
            new IntegerOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("item1"));
      assertThat(count).isEqualTo(3L);

      // Try adding with ADDNX - should return 0 and not increase count
      Long result = redis.dispatch(
            command("CF.ADDNX"),
            new IntegerOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("item1"));
      assertThat(result).isEqualTo(0L);

      count = redis.dispatch(
            command("CF.COUNT"),
            new IntegerOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("item1"));
      assertThat(count).isEqualTo(3L); // Still 3, not increased
   }

   @Test
   public void testCfInsertNxNocreate() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // INSERTNX with NOCREATE on non-existent key - should fail
      assertThatThrownBy(() -> redis.dispatch(
            command("CF.INSERTNX"),
            new ArrayOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("NOCREATE").add("ITEMS").add("item1")))
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("not found");
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
