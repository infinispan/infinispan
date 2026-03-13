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

@Test(groups = "functional", testName = "server.resp.BloomFilterTest")
public class BloomFilterTest extends SingleNodeRespBaseTest {

   @Test
   public void testBfReserve() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // Create a bloom filter with BF.RESERVE
      String result = redis.dispatch(
            command("BF.RESERVE"),
            new StatusOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("0.01").add("1000"));
      assertThat(result).isEqualTo("OK");

      // Try to reserve the same key again - should fail
      assertThatThrownBy(() -> redis.dispatch(
            command("BF.RESERVE"),
            new StatusOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("0.01").add("1000")))
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("item exists");
   }

   @Test
   public void testBfReserveWithOptions() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // Create with EXPANSION option
      String result = redis.dispatch(
            command("BF.RESERVE"),
            new StatusOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("0.01").add("1000").add("EXPANSION").add("4"));
      assertThat(result).isEqualTo("OK");
   }

   @Test
   public void testBfReserveNonScaling() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // Create with NONSCALING option
      String result = redis.dispatch(
            command("BF.RESERVE"),
            new StatusOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("0.01").add("1000").add("NONSCALING"));
      assertThat(result).isEqualTo("OK");
   }

   @Test
   public void testBfAdd() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // Add an item - filter will be created automatically
      Long result = redis.dispatch(
            command("BF.ADD"),
            new IntegerOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("item1"));
      assertThat(result).isEqualTo(1L);

      // Add the same item again - should return 0
      result = redis.dispatch(
            command("BF.ADD"),
            new IntegerOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("item1"));
      assertThat(result).isEqualTo(0L);

      // Add a different item - should return 1
      result = redis.dispatch(
            command("BF.ADD"),
            new IntegerOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("item2"));
      assertThat(result).isEqualTo(1L);
   }

   @Test
   public void testBfMadd() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // Add multiple items at once
      List<Object> results = redis.dispatch(
            command("BF.MADD"),
            new ArrayOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("item1").add("item2").add("item3"));
      assertThat(results).hasSize(3);
      assertThat(results).containsExactly(1L, 1L, 1L);

      // Add same items again - all should return 0
      results = redis.dispatch(
            command("BF.MADD"),
            new ArrayOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("item1").add("item2").add("item3"));
      assertThat(results).containsExactly(0L, 0L, 0L);

      // Mix of new and existing items
      results = redis.dispatch(
            command("BF.MADD"),
            new ArrayOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("item1").add("item4").add("item2"));
      assertThat(results).containsExactly(0L, 1L, 0L);
   }

   @Test
   public void testBfExists() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // Check non-existent key - should return 0
      Long result = redis.dispatch(
            command("BF.EXISTS"),
            new IntegerOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("item1"));
      assertThat(result).isEqualTo(0L);

      // Add item
      redis.dispatch(
            command("BF.ADD"),
            new IntegerOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("item1"));

      // Check existing item - should return 1
      result = redis.dispatch(
            command("BF.EXISTS"),
            new IntegerOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("item1"));
      assertThat(result).isEqualTo(1L);

      // Check non-existing item in existing filter - should return 0
      result = redis.dispatch(
            command("BF.EXISTS"),
            new IntegerOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("item2"));
      assertThat(result).isEqualTo(0L);
   }

   @Test
   public void testBfMexists() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // Add some items
      redis.dispatch(
            command("BF.MADD"),
            new ArrayOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("item1").add("item2"));

      // Check multiple items
      List<Object> results = redis.dispatch(
            command("BF.MEXISTS"),
            new ArrayOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("item1").add("item2").add("item3"));
      assertThat(results).containsExactly(1L, 1L, 0L);
   }

   @Test
   public void testBfMexistsNonExistentKey() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // Check items on non-existent key - all should return 0
      List<Object> results = redis.dispatch(
            command("BF.MEXISTS"),
            new ArrayOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("item1").add("item2"));
      assertThat(results).containsExactly(0L, 0L);
   }

   @Test
   public void testBfInsert() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // Insert with default options
      List<Object> results = redis.dispatch(
            command("BF.INSERT"),
            new ArrayOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("ITEMS").add("item1").add("item2"));
      assertThat(results).containsExactly(1L, 1L);

      // Insert again - existing items should return 0
      results = redis.dispatch(
            command("BF.INSERT"),
            new ArrayOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("ITEMS").add("item1").add("item3"));
      assertThat(results).containsExactly(0L, 1L);
   }

   @Test
   public void testBfInsertWithCapacity() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // Insert with custom capacity
      List<Object> results = redis.dispatch(
            command("BF.INSERT"),
            new ArrayOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("CAPACITY").add("500").add("ITEMS").add("item1"));
      assertThat(results).containsExactly(1L);
   }

   @Test
   public void testBfInsertWithError() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // Insert with custom error rate
      List<Object> results = redis.dispatch(
            command("BF.INSERT"),
            new ArrayOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("ERROR").add("0.001").add("ITEMS").add("item1"));
      assertThat(results).containsExactly(1L);
   }

   @Test
   public void testBfInsertNocreate() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // Insert with NOCREATE on non-existent key - should fail
      assertThatThrownBy(() -> redis.dispatch(
            command("BF.INSERT"),
            new ArrayOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("NOCREATE").add("ITEMS").add("item1")))
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("not found");

      // Create filter first
      redis.dispatch(
            command("BF.ADD"),
            new IntegerOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("item1"));

      // Now NOCREATE should work
      List<Object> results = redis.dispatch(
            command("BF.INSERT"),
            new ArrayOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("NOCREATE").add("ITEMS").add("item2"));
      assertThat(results).containsExactly(1L);
   }

   @Test
   public void testBfInsertWithExpansion() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // Insert with custom expansion rate
      List<Object> results = redis.dispatch(
            command("BF.INSERT"),
            new ArrayOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k())
                  .add("CAPACITY").add("100")
                  .add("EXPANSION").add("4")
                  .add("ITEMS").add("item1"));
      assertThat(results).containsExactly(1L);

      // Verify expansion rate via BF.INFO
      List<Object> info = redis.dispatch(
            command("BF.INFO"),
            new ArrayOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()));
      // Find expansion rate in info
      for (int i = 0; i < info.size() - 1; i++) {
         if ("Expansion rate".equals(info.get(i))) {
            assertThat(info.get(i + 1)).isEqualTo(4L);
            break;
         }
      }
   }

   @Test
   public void testBfInsertNonScaling() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // Insert with NONSCALING option
      List<Object> results = redis.dispatch(
            command("BF.INSERT"),
            new ArrayOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k())
                  .add("CAPACITY").add("100")
                  .add("NONSCALING")
                  .add("ITEMS").add("item1"));
      assertThat(results).containsExactly(1L);
   }

   @Test
   public void testBfCard() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // Card on non-existent key should return 0
      Long result = redis.dispatch(
            command("BF.CARD"),
            new IntegerOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()));
      assertThat(result).isEqualTo(0L);

      // Add items
      redis.dispatch(
            command("BF.MADD"),
            new ArrayOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("item1").add("item2").add("item3"));

      // Card should return 3
      result = redis.dispatch(
            command("BF.CARD"),
            new IntegerOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()));
      assertThat(result).isEqualTo(3L);

      // Add duplicate - card should still be 3
      redis.dispatch(
            command("BF.ADD"),
            new IntegerOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("item1"));

      result = redis.dispatch(
            command("BF.CARD"),
            new IntegerOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()));
      assertThat(result).isEqualTo(3L);
   }

   @Test
   public void testBfInfo() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // Create filter with specific params
      redis.dispatch(
            command("BF.RESERVE"),
            new StatusOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("0.01").add("1000").add("EXPANSION").add("4"));

      // Add some items
      redis.dispatch(
            command("BF.MADD"),
            new ArrayOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("item1").add("item2").add("item3"));

      // Get all info
      List<Object> info = redis.dispatch(
            command("BF.INFO"),
            new ArrayOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()));

      // Should have 10 elements (5 key-value pairs)
      assertThat(info).hasSize(10);
      assertThat(info.get(0)).isEqualTo("Capacity");
      assertThat(info.get(2)).isEqualTo("Size");
      assertThat(info.get(4)).isEqualTo("Number of filters");
      assertThat(info.get(6)).isEqualTo("Number of items inserted");
      assertThat(info.get(8)).isEqualTo("Expansion rate");

      // Verify item count
      assertThat(info.get(7)).isEqualTo(3L);
      // Verify expansion rate
      assertThat(info.get(9)).isEqualTo(4L);
   }

   @Test
   public void testBfInfoSpecificField() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // Create filter
      redis.dispatch(
            command("BF.RESERVE"),
            new StatusOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("0.01").add("1000"));

      redis.dispatch(
            command("BF.MADD"),
            new ArrayOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("item1").add("item2"));

      // Get specific field - CAPACITY
      List<Object> info = redis.dispatch(
            command("BF.INFO"),
            new ArrayOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("CAPACITY"));
      assertThat(info).hasSize(1);
      assertThat((Long) info.get(0)).isEqualTo(1000L);

      // Get specific field - ITEMS
      info = redis.dispatch(
            command("BF.INFO"),
            new ArrayOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("ITEMS"));
      assertThat(info).hasSize(1);
      assertThat((Long) info.get(0)).isEqualTo(2L);

      // Get specific field - FILTERS
      info = redis.dispatch(
            command("BF.INFO"),
            new ArrayOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("FILTERS"));
      assertThat(info).hasSize(1);
      assertThat((Long) info.get(0)).isEqualTo(1L);
   }

   @Test
   public void testBfInfoNonExistentKey() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // Info on non-existent key should fail
      assertThatThrownBy(() -> redis.dispatch(
            command("BF.INFO"),
            new ArrayOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k())))
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("not found");
   }

   @Test
   public void testBfInfoInvalidParameter() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // Create filter
      redis.dispatch(
            command("BF.RESERVE"),
            new StatusOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("0.01").add("1000"));

      // Invalid parameter should fail
      assertThatThrownBy(() -> redis.dispatch(
            command("BF.INFO"),
            new ArrayOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("INVALID")))
            .isInstanceOf(RedisCommandExecutionException.class);
   }

   @Test
   public void testBfReserveInvalidParams() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // Invalid error rate (too high)
      assertThatThrownBy(() -> redis.dispatch(
            command("BF.RESERVE"),
            new StatusOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("1.5").add("1000")))
            .isInstanceOf(RedisCommandExecutionException.class);

      // Invalid error rate (negative)
      assertThatThrownBy(() -> redis.dispatch(
            command("BF.RESERVE"),
            new StatusOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("-0.01").add("1000")))
            .isInstanceOf(RedisCommandExecutionException.class);

      // Invalid capacity (zero)
      assertThatThrownBy(() -> redis.dispatch(
            command("BF.RESERVE"),
            new StatusOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("0.01").add("0")))
            .isInstanceOf(RedisCommandExecutionException.class);
   }

   @Test
   public void testBfReserveErrorRateOne() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // Error rate of exactly 1.0 should be rejected
      assertThatThrownBy(() -> redis.dispatch(
            command("BF.RESERVE"),
            new StatusOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("1.0").add("1000")))
            .isInstanceOf(RedisCommandExecutionException.class);
   }

   @Test
   public void testBfReserveInvalidExpansion() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // Invalid expansion (zero)
      assertThatThrownBy(() -> redis.dispatch(
            command("BF.RESERVE"),
            new StatusOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("0.01").add("1000").add("EXPANSION").add("0")))
            .isInstanceOf(RedisCommandExecutionException.class);

      // Invalid expansion (negative)
      assertThatThrownBy(() -> redis.dispatch(
            command("BF.RESERVE"),
            new StatusOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k() + "2").add("0.01").add("1000").add("EXPANSION").add("-1")))
            .isInstanceOf(RedisCommandExecutionException.class);
   }

   @Test
   public void testBfExpansion() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // Create a small filter that will need to expand
      redis.dispatch(
            command("BF.RESERVE"),
            new StatusOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("0.01").add("10").add("EXPANSION").add("2"));

      // Add more items than the initial capacity
      for (int i = 0; i < 50; i++) {
         redis.dispatch(
               command("BF.ADD"),
               new IntegerOutput<>(StringCodec.UTF8),
               new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("item" + i));
      }

      // Check that filter count increased (expansion occurred)
      List<Object> info = redis.dispatch(
            command("BF.INFO"),
            new ArrayOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()));

      // Find number of filters in info
      Long filterCount = null;
      for (int i = 0; i < info.size() - 1; i++) {
         if ("Number of filters".equals(info.get(i))) {
            filterCount = (Long) info.get(i + 1);
            break;
         }
      }
      assertThat(filterCount).isNotNull();
      assertThat(filterCount).isGreaterThan(1L); // Should have expanded

      // Verify all items are still found
      for (int i = 0; i < 50; i++) {
         Long result = redis.dispatch(
               command("BF.EXISTS"),
               new IntegerOutput<>(StringCodec.UTF8),
               new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("item" + i));
         assertThat(result).isEqualTo(1L);
      }
   }

   @Test
   public void testBfNonScalingCapacityLimit() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // Create a non-scaling filter with small capacity
      redis.dispatch(
            command("BF.RESERVE"),
            new StatusOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("0.01").add("10").add("NONSCALING"));

      // Add items up to and beyond capacity - should eventually fail or stop adding
      // Note: Bloom filters may allow slightly more items than capacity before failing
      boolean gotError = false;
      for (int i = 0; i < 100; i++) {
         try {
            redis.dispatch(
                  command("BF.ADD"),
                  new IntegerOutput<>(StringCodec.UTF8),
                  new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("item" + i));
         } catch (RedisCommandExecutionException e) {
            gotError = true;
            assertThat(e.getMessage()).containsIgnoringCase("full");
            break;
         }
      }
      assertThat(gotError).isTrue();
   }

   @Test
   public void testBfWrongType() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // Create a string key
      redis.set(k(), "value");

      // BF.ADD on wrong type should fail
      assertThatThrownBy(() -> redis.dispatch(
            command("BF.ADD"),
            new IntegerOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("item1")))
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("WRONGTYPE");

      // BF.EXISTS on wrong type should fail
      assertThatThrownBy(() -> redis.dispatch(
            command("BF.EXISTS"),
            new IntegerOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("item1")))
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("WRONGTYPE");

      // BF.INFO on wrong type should fail
      assertThatThrownBy(() -> redis.dispatch(
            command("BF.INFO"),
            new ArrayOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k())))
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("WRONGTYPE");
   }

   @Test
   public void testBfVeryHighErrorRate() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // Create filter with very high (but valid) error rate - close to 1.0
      redis.dispatch(
            command("BF.RESERVE"),
            new StatusOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("0.99").add("100"));

      // Add items
      for (int i = 0; i < 10; i++) {
         redis.dispatch(
               command("BF.ADD"),
               new IntegerOutput<>(StringCodec.UTF8),
               new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("item" + i));
      }

      // Items should still be found (no false negatives)
      for (int i = 0; i < 10; i++) {
         Long result = redis.dispatch(
               command("BF.EXISTS"),
               new IntegerOutput<>(StringCodec.UTF8),
               new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("item" + i));
         assertThat(result).isEqualTo(1L);
      }
   }

   @Test
   public void testBloomFilterFalsePositiveRate() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // Create a small filter with 1% error rate
      redis.dispatch(
            command("BF.RESERVE"),
            new StatusOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("0.01").add("1000"));

      // Add items
      for (int i = 0; i < 100; i++) {
         redis.dispatch(
               command("BF.ADD"),
               new IntegerOutput<>(StringCodec.UTF8),
               new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("item" + i));
      }

      // Check that added items are found
      for (int i = 0; i < 100; i++) {
         Long result = redis.dispatch(
               command("BF.EXISTS"),
               new IntegerOutput<>(StringCodec.UTF8),
               new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("item" + i));
         assertThat(result).isEqualTo(1L);
      }

      // Check false positive rate - should be roughly 1%
      int falsePositives = 0;
      for (int i = 100; i < 200; i++) {
         Long result = redis.dispatch(
               command("BF.EXISTS"),
               new IntegerOutput<>(StringCodec.UTF8),
               new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("nonexistent" + i));
         if (result == 1L) {
            falsePositives++;
         }
      }
      // False positive rate should be less than 10% (much higher than 1% to account for variance)
      assertThat(falsePositives).isLessThan(10);
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
