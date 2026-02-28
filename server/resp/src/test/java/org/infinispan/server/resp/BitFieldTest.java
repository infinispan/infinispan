package org.infinispan.server.resp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.infinispan.test.TestingUtil.k;

import java.nio.charset.StandardCharsets;
import java.util.List;

import org.infinispan.server.resp.logging.Messages;
import org.testng.annotations.Test;

import io.lettuce.core.BitFieldArgs;
import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.output.ArrayOutput;
import io.lettuce.core.protocol.CommandArgs;
import io.lettuce.core.protocol.CommandType;

@Test(groups = "functional", testName = "server.resp.BitFieldTest")
public class BitFieldTest extends SingleNodeRespBaseTest {
   @Override
   protected RedisCodec<String, String> newCodec() {
      return new StringCodec(StandardCharsets.ISO_8859_1);
   }

   @Test
   public void testBitfieldGet() {
      RedisCommands<String, String> redis = redisConnection.sync();
      redis.set(k(), "\u0001\u0002\u0003");

      List<Long> values = redis.bitfield(k(), BitFieldArgs.Builder.get(BitFieldArgs.unsigned(8), 0));
      assertThat(values).containsExactly(1L);

      values = redis.bitfield(k(), BitFieldArgs.Builder.get(BitFieldArgs.unsigned(8), 8));
      assertThat(values).containsExactly(2L);

      values = redis.bitfield(k(), BitFieldArgs.Builder.get(BitFieldArgs.unsigned(8), 16));
      assertThat(values).containsExactly(3L);
   }

   @Test
   public void testBitfieldSet() {
      RedisCommands<String, String> redis = redisConnection.sync();
      redis.set(k(), "\u0000\u0000\u0000");

      redis.bitfield(k(), BitFieldArgs.Builder.set(BitFieldArgs.unsigned(8), 0, 1));
      assertThat(redis.get(k())).isEqualTo("\u0001\u0000\u0000");

      redis.bitfield(k(), BitFieldArgs.Builder.set(BitFieldArgs.unsigned(8), 8, 2));
      assertThat(redis.get(k())).isEqualTo("\u0001\u0002\u0000");

      redis.bitfield(k(), BitFieldArgs.Builder.set(BitFieldArgs.unsigned(8), 16, 3));
      assertThat(redis.get(k())).isEqualTo("\u0001\u0002\u0003");
   }

   @Test
   public void testBitfieldGetSet() {
      RedisCommands<String, String> redis = redisConnection.sync();
      assertThat(redis.bitfield(k(), BitFieldArgs.Builder.set(BitFieldArgs.signed(8), 0, 255).set(BitFieldArgs.signed(8), 0, 100).get(BitFieldArgs.signed(8), 0))).containsExactly(0L, -1L, 100L);
   }

   @Test
   public void testBitfieldIncrby() {
      RedisCommands<String, String> redis = redisConnection.sync();
      redis.set(k(), "\u0000\u0000\u0000");

      List<Long> values = redis.bitfield(k(), BitFieldArgs.Builder.incrBy(BitFieldArgs.unsigned(8), 0, 1));
      assertThat(values).containsExactly(1L);
      assertThat(redis.get(k())).isEqualTo("\u0001\u0000\u0000");

      values = redis.bitfield(k(), BitFieldArgs.Builder.incrBy(BitFieldArgs.unsigned(8), 8, 2));
      assertThat(values).containsExactly(2L);
      assertThat(redis.get(k())).isEqualTo("\u0001\u0002\u0000");

      values = redis.bitfield(k(), BitFieldArgs.Builder.incrBy(BitFieldArgs.unsigned(8), 16, 3));
      assertThat(values).containsExactly(3L);
      assertThat(redis.get(k())).isEqualTo("\u0001\u0002\u0003");
   }

   @Test
   public void testBitfieldOverflow() {
      RedisCommands<String, String> redis = redisConnection.sync();

      redis.del(k());
      redis.bitfield(k(), BitFieldArgs.Builder.set(BitFieldArgs.unsigned(8), 0, 100));
      assertThat(redis.bitfield(k(), BitFieldArgs.Builder.overflow(BitFieldArgs.OverflowType.WRAP).incrBy(BitFieldArgs.unsigned(8), 0, 257))).containsExactly(101L);
      assertThat(redis.bitfield(k(), BitFieldArgs.Builder.get(BitFieldArgs.unsigned(8), 0))).containsExactly(101L);
      assertThat(redis.bitfield(k(), BitFieldArgs.Builder.overflow(BitFieldArgs.OverflowType.WRAP).incrBy(BitFieldArgs.unsigned(8), 0, 255))).containsExactly(100L);
      assertThat(redis.bitfield(k(), BitFieldArgs.Builder.get(BitFieldArgs.unsigned(8), 0))).containsExactly(100L);

      redis.del(k());
      redis.bitfield(k(), BitFieldArgs.Builder.set(BitFieldArgs.signed(8), 0, 100));
      assertThat(redis.bitfield(k(), BitFieldArgs.Builder.overflow(BitFieldArgs.OverflowType.WRAP).incrBy(BitFieldArgs.signed(8), 0, 257))).containsExactly(101L);
      assertThat(redis.bitfield(k(), BitFieldArgs.Builder.get(BitFieldArgs.signed(8), 0))).containsExactly(101L);
      assertThat(redis.bitfield(k(), BitFieldArgs.Builder.overflow(BitFieldArgs.OverflowType.WRAP).incrBy(BitFieldArgs.signed(8), 0, 255))).containsExactly(100L);
      assertThat(redis.bitfield(k(), BitFieldArgs.Builder.get(BitFieldArgs.signed(8), 0))).containsExactly(100L);

      redis.del(k());
      redis.bitfield(k(), BitFieldArgs.Builder.set(BitFieldArgs.unsigned(8), 0, 100));
      assertThat(redis.bitfield(k(), BitFieldArgs.Builder.overflow(BitFieldArgs.OverflowType.SAT).incrBy(BitFieldArgs.unsigned(8), 0, 257))).containsExactly(255L);
      assertThat(redis.bitfield(k(), BitFieldArgs.Builder.get(BitFieldArgs.unsigned(8), 0))).containsExactly(255L);
      assertThat(redis.bitfield(k(), BitFieldArgs.Builder.overflow(BitFieldArgs.OverflowType.SAT).incrBy(BitFieldArgs.unsigned(8), 0, -255))).containsExactly(0L);
      assertThat(redis.bitfield(k(), BitFieldArgs.Builder.get(BitFieldArgs.unsigned(8), 0))).containsExactly(0L);

      redis.del(k());
      redis.bitfield(k(), BitFieldArgs.Builder.set(BitFieldArgs.unsigned(8), 0, 100));
      assertThat(redis.bitfield(k(), BitFieldArgs.Builder.overflow(BitFieldArgs.OverflowType.SAT).incrBy(BitFieldArgs.signed(8), 0, 257))).containsExactly(127L);
      assertThat(redis.bitfield(k(), BitFieldArgs.Builder.get(BitFieldArgs.signed(8), 0))).containsExactly(127L);
      assertThat(redis.bitfield(k(), BitFieldArgs.Builder.overflow(BitFieldArgs.OverflowType.SAT).incrBy(BitFieldArgs.signed(8), 0, -255))).containsExactly(-128L);
      assertThat(redis.bitfield(k(), BitFieldArgs.Builder.get(BitFieldArgs.signed(8), 0))).containsExactly(-128L);
   }

   @Test
   public void testBitfieldInvalidArguments() {
      RedisCommands<String, String> redis = redisConnection.sync();

      assertThatThrownBy(() -> redis.dispatch(CommandType.BITFIELD, new ArrayOutput<>(StringCodec.UTF8), new CommandArgs<>(StringCodec.UTF8).add(k()).add("set").add("u64").add(0).add(1))).isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining(Messages.MESSAGES.invalidBitfieldType());

      assertThatThrownBy(() -> redis.dispatch(CommandType.BITFIELD, new ArrayOutput<>(StringCodec.UTF8), new CommandArgs<>(StringCodec.UTF8).add(k()).add("set").add("i65").add(0).add(1))).isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining(Messages.MESSAGES.invalidBitfieldType());

      assertThatThrownBy(() -> redis.dispatch(CommandType.BITFIELD, new ArrayOutput<>(StringCodec.UTF8), new CommandArgs<>(StringCodec.UTF8).add(k()).add("set").add("u0").add(0).add(1))).isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining(Messages.MESSAGES.invalidBitfieldType());

      assertThatThrownBy(() -> redis.dispatch(CommandType.BITFIELD, new ArrayOutput<>(StringCodec.UTF8), new CommandArgs<>(StringCodec.UTF8).add(k()).add("set").add("x1").add(0).add(1))).isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining(Messages.MESSAGES.invalidBitfieldType());

      assertThatThrownBy(() -> redis.dispatch(CommandType.BITFIELD, new ArrayOutput<>(StringCodec.UTF8), new CommandArgs<>(StringCodec.UTF8).add(k()).add("set").add("u8").add(-1).add(1))).isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining(Messages.MESSAGES.invalidBitOffset());
   }

   @Test
   public void testBitfieldMulti() {
      RedisCommands<String, String> redis = redisConnection.sync();
      redis.set(k(), "\u0000\u0000\u0000");

      List<Long> values = redis.bitfield(k(),
            BitFieldArgs.Builder
                  .incrBy(BitFieldArgs.unsigned(8), 0, 1)
                  .get(BitFieldArgs.unsigned(8), 0)
                  .incrBy(BitFieldArgs.unsigned(8), 8, 2)
                  .get(BitFieldArgs.unsigned(8), 8)
                  .incrBy(BitFieldArgs.unsigned(8), 16, 3)
                  .get(BitFieldArgs.unsigned(8), 16)
                  .set(BitFieldArgs.unsigned(8), 0, 255)
                  .get(BitFieldArgs.unsigned(8), 0)
                  .overflow(BitFieldArgs.OverflowType.WRAP)
                  .incrBy(BitFieldArgs.unsigned(8), 0, 1)
                  .get(BitFieldArgs.unsigned(8), 0)
      );
      assertThat(values).containsExactly(1L, 1L, 2L, 2L, 3L, 3L, 1L, 255L, 0L, 0L);
      assertThat(redis.get(k())).isEqualTo("\u0000\u0002\u0003");
   }

   @Test
   public void testBitfieldSignedSetGetBasics() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // SET 255 in signed 8-bit should read back as -1
      redis.bitfield(k(), BitFieldArgs.Builder.set(BitFieldArgs.signed(8), 0, 255));
      List<Long> values = redis.bitfield(k(), BitFieldArgs.Builder.get(BitFieldArgs.signed(8), 0));
      assertThat(values).containsExactly(-1L);

      // SET -128 in signed 8-bit should read back as -128
      redis.bitfield(k(), BitFieldArgs.Builder.set(BitFieldArgs.signed(8), 0, -128));
      values = redis.bitfield(k(), BitFieldArgs.Builder.get(BitFieldArgs.signed(8), 0));
      assertThat(values).containsExactly(-128L);

      // SET 127 in signed 8-bit should read back as 127
      redis.bitfield(k(), BitFieldArgs.Builder.set(BitFieldArgs.signed(8), 0, 127));
      values = redis.bitfield(k(), BitFieldArgs.Builder.get(BitFieldArgs.signed(8), 0));
      assertThat(values).containsExactly(127L);
   }

   @Test
   public void testBitfieldUnsignedSetGetIncrby() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // Combine SET, GET, and INCRBY on unsigned fields
      List<Long> values = redis.bitfield(k(),
            BitFieldArgs.Builder
                  .set(BitFieldArgs.unsigned(8), 0, 10)
                  .get(BitFieldArgs.unsigned(8), 0)
                  .incrBy(BitFieldArgs.unsigned(8), 0, 100)
                  .get(BitFieldArgs.unsigned(8), 0));
      // SET returns old value (0), GET returns 10, INCRBY returns 110, GET returns 110
      assertThat(values).containsExactly(0L, 10L, 110L, 110L);
   }

   @Test
   public void testBitfieldHashIndexForm() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // Use #<idx> form via raw dispatch
      // #0 for u8 = bit 0, #1 for u8 = bit 8
      redis.dispatch(CommandType.BITFIELD, new ArrayOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("SET").add("u8").add("#0").add("65"));
      redis.dispatch(CommandType.BITFIELD, new ArrayOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("SET").add("u8").add("#1").add("66"));

      List<Object> result = redis.dispatch(CommandType.BITFIELD, new ArrayOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("GET").add("u8").add("#0"));
      assertThat(result).containsExactly(65L);

      result = redis.dispatch(CommandType.BITFIELD, new ArrayOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("GET").add("u8").add("#1"));
      assertThat(result).containsExactly(66L);

      // The underlying string should be "AB" (65='A', 66='B')
      assertThat(redis.get(k())).isEqualTo("AB");
   }

   @Test
   public void testBitfieldHashIndexIncrby() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // Use #<idx> form with INCRBY
      redis.dispatch(CommandType.BITFIELD, new ArrayOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("SET").add("u8").add("#0").add("10"));

      List<Object> result = redis.dispatch(CommandType.BITFIELD, new ArrayOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("INCRBY").add("u8").add("#0").add("5"));
      assertThat(result).containsExactly(15L);
   }

   @Test
   public void testBitfieldOnNonExistentKey() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // SET and INCRBY on non-existent key (regression for Redis #3564)
      List<Long> values = redis.bitfield(k(),
            BitFieldArgs.Builder
                  .set(BitFieldArgs.unsigned(8), 0, 100)
                  .incrBy(BitFieldArgs.unsigned(8), 0, 100));
      // SET returns previous value (0), INCRBY returns new value (200)
      assertThat(values).containsExactly(0L, 200L);
   }

   @Test
   public void testBitfieldWrongType() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // Create a list key
      redis.rpush(k(), "value");

      // BITFIELD GET on wrong type should fail
      assertThatThrownBy(() -> redis.bitfield(k(),
            BitFieldArgs.Builder.get(BitFieldArgs.unsigned(8), 0)))
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("WRONGTYPE");
   }

   @Test
   public void testBitfieldUnsignedFailOverflow() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // FAIL mode for unsigned: should return null when overflow
      redis.del(k());
      redis.bitfield(k(), BitFieldArgs.Builder.set(BitFieldArgs.unsigned(8), 0, 200));

      // Incrementing past 255 should fail
      List<Long> result = redis.bitfield(k(),
            BitFieldArgs.Builder.overflow(BitFieldArgs.OverflowType.FAIL)
                  .incrBy(BitFieldArgs.unsigned(8), 0, 100));
      assertThat(result).containsExactly((Long) null);

      // Value should remain unchanged
      assertThat(redis.bitfield(k(), BitFieldArgs.Builder.get(BitFieldArgs.unsigned(8), 0)))
            .containsExactly(200L);

      // Decrementing below 0 should fail
      redis.del(k());
      redis.bitfield(k(), BitFieldArgs.Builder.set(BitFieldArgs.unsigned(8), 0, 10));
      result = redis.bitfield(k(),
            BitFieldArgs.Builder.overflow(BitFieldArgs.OverflowType.FAIL)
                  .incrBy(BitFieldArgs.unsigned(8), 0, -20));
      assertThat(result).containsExactly((Long) null);

      // Value should remain unchanged
      assertThat(redis.bitfield(k(), BitFieldArgs.Builder.get(BitFieldArgs.unsigned(8), 0)))
            .containsExactly(10L);

      // Within range should succeed
      redis.del(k());
      redis.bitfield(k(), BitFieldArgs.Builder.set(BitFieldArgs.unsigned(8), 0, 100));
      result = redis.bitfield(k(),
            BitFieldArgs.Builder.overflow(BitFieldArgs.OverflowType.FAIL)
                  .incrBy(BitFieldArgs.unsigned(8), 0, 50));
      assertThat(result).containsExactly(150L);
   }

   @Test
   public void testBitfieldRoGet() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // Set up data using BITFIELD
      redis.bitfield(k(), BitFieldArgs.Builder.set(BitFieldArgs.unsigned(8), 0, 100));

      // Read using BITFIELD_RO
      List<Object> result = redis.dispatch(bitfieldRo(), new ArrayOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("GET").add("u8").add("0"));
      assertThat(result).containsExactly(100L);
   }

   @Test
   public void testBitfieldRoRejectsWrite() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // BITFIELD_RO with SET should fail
      assertThatThrownBy(() -> redis.dispatch(bitfieldRo(), new ArrayOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("SET").add("u8").add("0").add("100")))
            .isInstanceOf(RedisCommandExecutionException.class);

      // BITFIELD_RO with INCRBY should fail
      assertThatThrownBy(() -> redis.dispatch(bitfieldRo(), new ArrayOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(k()).add("INCRBY").add("u8").add("0").add("100")))
            .isInstanceOf(RedisCommandExecutionException.class);
   }

   private static io.lettuce.core.protocol.ProtocolKeyword bitfieldRo() {
      return new io.lettuce.core.protocol.ProtocolKeyword() {
         @Override
         public byte[] getBytes() {
            return "BITFIELD_RO".getBytes();
         }

         @Override
         public String name() {
            return "BITFIELD_RO";
         }
      };
   }
}
