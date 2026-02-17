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
}
