package org.infinispan.server.resp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.test.TestingUtil.k;

import java.nio.charset.StandardCharsets;

import org.testng.annotations.Test;

import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;

@Test(groups = "functional", testName = "server.resp.BitOpsTest")
public class BitOpsTest extends SingleNodeRespBaseTest {

   @Override
   protected RedisCodec<String, String> newCodec() {
      return new StringCodec(StandardCharsets.ISO_8859_1);
   }

   @Test
   public void testGetBit() {
      RedisCommands<String, String> redis = redisConnection.sync();
      redis.set(k(), "foobar");

      assertThat(redis.getbit(k(), 0)).isEqualTo(0);
      assertThat(redis.getbit(k(), 1)).isEqualTo(1);
      assertThat(redis.getbit(k(), 2)).isEqualTo(1);
      assertThat(redis.getbit(k(), 3)).isEqualTo(0);
      assertThat(redis.getbit(k(), 4)).isEqualTo(0);
      assertThat(redis.getbit(k(), 5)).isEqualTo(1);
      assertThat(redis.getbit(k(), 6)).isEqualTo(1);
      assertThat(redis.getbit(k(), 7)).isEqualTo(0);

      assertThat(redis.getbit(k(), 100)).isEqualTo(0);
   }

   @Test
   public void testSetBit() {
      RedisCommands<String, String> redis = redisConnection.sync();
      redis.set(k(), "foobar");
      assertThat(redis.setbit(k(), 4, 1)).isEqualTo(0);
      String v = redis.get(k());
      assertThat(v.getBytes()[0]).isEqualTo((byte) ('f' | 0b1000));
   }

   @Test
   public void testBitCount() {
      RedisCommands<String, String> redis = redisConnection.sync();
      redis.set(k(), "foobar");

      assertThat(redis.bitcount(k())).isEqualTo(26);
      assertThat(redis.bitcount(k(), 0, 0)).isEqualTo(4);
      assertThat(redis.bitcount(k(), 1, 1)).isEqualTo(6);
   }

   @Test
   public void testBitPos() {
      RedisCommands<String, String> redis = redisConnection.sync();
      redis.set(k(), "\u00ff\u00f0\u0000");
      assertThat(redis.bitpos(k(), false)).isEqualTo(12);

      redis.set(k(), "\u0000\u00ff\u00f0");
      assertThat(redis.bitpos(k(), true, 0, -1)).isEqualTo(8);
      assertThat(redis.bitpos(k(), true, 2, -1)).isEqualTo(16);
   }

   @Test
   public void testBitOp() {
      RedisCommands<String, String> redis = redisConnection.sync();
      redis.set(k(1), "foo");
      redis.set(k(2), "bar");

      redis.bitopAnd(k(3), k(1), k(2));
      assertThat(redis.get(k(3))).isEqualTo("bab");

      redis.set(k(1), "foo");
      redis.set(k(2), "bar");

      redis.bitopOr(k(3), k(1), k(2));
      assertThat(redis.get(k(3))).isEqualTo("fo\u007f");

      redis.set(k(1), "foo");
      redis.set(k(2), "bar");

      redis.bitopXor(k(3), k(1), k(2));
      assertThat(redis.get(k(3)).getBytes(StandardCharsets.ISO_8859_1)).isEqualTo(new byte[]{0x4, 0xe, 0x1d});

      redis.set(k(1), "foo");
      redis.bitopNot(k(2), k(1));
      assertThat(redis.get(k(2))).isEqualTo("\u0099\u0090\u0090");
   }
}
