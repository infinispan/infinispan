package org.infinispan.server.resp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.test.TestingUtil.k;

import java.nio.charset.StandardCharsets;
import java.util.List;

import org.testng.annotations.Test;

import io.lettuce.core.BitFieldArgs;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;

/**
 * Test case to demonstrate overflow bugs in BitfieldOperation
 */
@Test(groups = "functional", testName = "server.resp.BitFieldOverflowBugTest")
public class BitFieldOverflowBugTest extends SingleNodeRespBaseTest {
   @Override
   protected RedisCodec<String, String> newCodec() {
      return new StringCodec(StandardCharsets.ISO_8859_1);
   }

   @Test
   public void testSignedWrapOverflow() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // Test wrapping from positive to negative
      redis.del(k());
      redis.bitfield(k(), BitFieldArgs.Builder.set(BitFieldArgs.signed(8), 0, 100));
      // 100 + 50 = 150, which wraps to -106 in signed 8-bit
      List<Long> result = redis.bitfield(k(),
         BitFieldArgs.Builder.overflow(BitFieldArgs.OverflowType.WRAP)
            .incrBy(BitFieldArgs.signed(8), 0, 50));
      assertThat(result).containsExactly(-106L);

      // Test wrapping from negative to positive
      redis.del(k());
      redis.bitfield(k(), BitFieldArgs.Builder.set(BitFieldArgs.signed(8), 0, -100));
      // -100 - 50 = -150, which wraps to 106 in signed 8-bit
      result = redis.bitfield(k(),
         BitFieldArgs.Builder.overflow(BitFieldArgs.OverflowType.WRAP)
            .incrBy(BitFieldArgs.signed(8), 0, -50));
      assertThat(result).containsExactly(106L);
   }

   @Test
   public void testSignedFailOverflow() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // Negative values should NOT fail for signed integers
      redis.del(k());
      redis.bitfield(k(), BitFieldArgs.Builder.set(BitFieldArgs.signed(8), 0, 0));
      List<Long> result = redis.bitfield(k(),
         BitFieldArgs.Builder.overflow(BitFieldArgs.OverflowType.FAIL)
            .incrBy(BitFieldArgs.signed(8), 0, -50));
      // Should succeed and return -50, NOT null
      assertThat(result).containsExactly(-50L);

      // Should fail when going above max (127)
      redis.del(k());
      redis.bitfield(k(), BitFieldArgs.Builder.set(BitFieldArgs.signed(8), 0, 100));
      result = redis.bitfield(k(),
         BitFieldArgs.Builder.overflow(BitFieldArgs.OverflowType.FAIL)
            .incrBy(BitFieldArgs.signed(8), 0, 50));

            assertThat(result).containsExactly((Long) null);

      // Should fail when going below min (-128)
      redis.del(k());
      redis.bitfield(k(), BitFieldArgs.Builder.set(BitFieldArgs.signed(8), 0, -100));
      result = redis.bitfield(k(),
         BitFieldArgs.Builder.overflow(BitFieldArgs.OverflowType.FAIL)
            .incrBy(BitFieldArgs.signed(8), 0, -50));
      assertThat(result).containsExactly((Long) null);
   }

   @Test
   public void testSignedSatOverflow() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // Test saturation at max (127)
      redis.del(k());
      redis.bitfield(k(), BitFieldArgs.Builder.set(BitFieldArgs.signed(8), 0, 100));
      List<Long> result = redis.bitfield(k(),
         BitFieldArgs.Builder.overflow(BitFieldArgs.OverflowType.SAT)
            .incrBy(BitFieldArgs.signed(8), 0, 50));
      assertThat(result).containsExactly(127L);

      // Test saturation at min (-128)
      redis.del(k());
      redis.bitfield(k(), BitFieldArgs.Builder.set(BitFieldArgs.signed(8), 0, -100));
      result = redis.bitfield(k(),
         BitFieldArgs.Builder.overflow(BitFieldArgs.OverflowType.SAT)
            .incrBy(BitFieldArgs.signed(8), 0, -50));
      assertThat(result).containsExactly(-128L);
   }
}
