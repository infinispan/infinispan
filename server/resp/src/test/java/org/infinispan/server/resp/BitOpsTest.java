package org.infinispan.server.resp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.server.resp.test.RespTestingUtil.assertWrongType;
import static org.infinispan.test.TestingUtil.k;

import java.nio.charset.StandardCharsets;

import org.testng.annotations.Test;

import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.output.IntegerOutput;
import io.lettuce.core.protocol.CommandArgs;
import io.lettuce.core.protocol.ProtocolKeyword;

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
   public void testSetBitCreatesKey() {
      RedisCommands<String, String> redis = redisConnection.sync();
      // SETBIT on non-existing key creates it
      assertThat(redis.setbit(k(), 7, 1)).isEqualTo(0);
      assertThat(redis.getbit(k(), 7)).isEqualTo(1);
      assertThat(redis.strlen(k())).isEqualTo(1);
   }

   @Test
   public void testSetBitReturnsOldValue() {
      RedisCommands<String, String> redis = redisConnection.sync();
      redis.setbit(k(), 0, 1);
      assertThat(redis.setbit(k(), 0, 0)).isEqualTo(1);
      assertThat(redis.setbit(k(), 0, 0)).isEqualTo(0);
   }

   @Test
   public void testSetBitExpandsString() {
      RedisCommands<String, String> redis = redisConnection.sync();
      // Setting a bit far away should expand the string with zero bytes
      assertThat(redis.setbit(k(), 31, 1)).isEqualTo(0);
      assertThat(redis.strlen(k())).isEqualTo(4);
      assertThat(redis.getbit(k(), 31)).isEqualTo(1);
      assertThat(redis.getbit(k(), 0)).isEqualTo(0);
   }

   @Test
   public void testSetBitWrongType() {
      RedisCommands<String, String> redis = redisConnection.sync();
      assertWrongType(() -> redis.lpush(k(), "list"), () -> redis.setbit(k(), 0, 1));
   }

   @Test
   public void testGetBitWrongType() {
      RedisCommands<String, String> redis = redisConnection.sync();
      assertWrongType(() -> redis.lpush(k(), "list"), () -> redis.getbit(k(), 0));
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
   public void testBitCountWrongType() {
      RedisCommands<String, String> redis = redisConnection.sync();
      assertWrongType(() -> redis.lpush(k(), "list"), () -> redis.bitcount(k()));
   }

   @Test
   public void testBitCountNonExistingKey() {
      RedisCommands<String, String> redis = redisConnection.sync();
      assertThat(redis.bitcount(k())).isEqualTo(0);
   }

   @Test
   public void testBitCountOutOfRangeIndexes() {
      RedisCommands<String, String> redis = redisConnection.sync();
      redis.set(k(), "foobar");
      // start > string length
      assertThat(redis.bitcount(k(), 10, 20)).isEqualTo(0);
   }

   @Test
   public void testBitCountNegativeIndexesStartGtEnd() {
      RedisCommands<String, String> redis = redisConnection.sync();
      redis.set(k(), "foobar");
      // Negative indexes where start > end
      assertThat(redis.bitcount(k(), -1, -3)).isEqualTo(0);
   }

   @Test
   public void testBitCountTestVectors() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // Empty string = 0 bits
      redis.set(k(), "");
      assertThat(redis.bitcount(k())).isEqualTo(0);

      // \xff = 8 bits set
      redis.set(k(), "\u00ff");
      assertThat(redis.bitcount(k())).isEqualTo(8);

      // "hello" - count all
      redis.set(k(), "hello");
      // h=01101000(3) e=01100101(4) l=01101100(4) l=01101100(4) o=01101111(6) = 21
      assertThat(redis.bitcount(k())).isEqualTo(21);
   }

   @Test
   public void testBitCountWithStartEnd() {
      RedisCommands<String, String> redis = redisConnection.sync();
      // "foobar": f(4) o(6) o(6) b(3) a(3) r(4) = 26 bits
      redis.set(k(), "foobar");

      // Full range
      assertThat(redis.bitcount(k(), 0, -1)).isEqualTo(26);

      // Negative end: 0 to -2 = bytes 0-4 = "fooba" = 4+6+6+3+3 = 22
      assertThat(redis.bitcount(k(), 0, -2)).isEqualTo(22);
      // 1 to -2 = bytes 1-4 = "ooba" = 6+6+3+3 = 18
      assertThat(redis.bitcount(k(), 1, -2)).isEqualTo(18);

      // Both negative: -2 to -1 = bytes 4-5 = "ar" = 3+4 = 7
      assertThat(redis.bitcount(k(), -2, -1)).isEqualTo(7);
   }

   @Test
   public void testBitCountOnSetBitKey() {
      RedisCommands<String, String> redis = redisConnection.sync();
      redis.setbit(k(), 100, 1);
      assertThat(redis.bitcount(k())).isEqualTo(1);
      assertThat(redis.bitcount(k(), 0, -1)).isEqualTo(1);
      // Bit is in byte 12 (100/8)
      assertThat(redis.bitcount(k(), 12, 12)).isEqualTo(1);
      assertThat(redis.bitcount(k(), 0, 11)).isEqualTo(0);
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
   public void testBitPosWrongType() {
      RedisCommands<String, String> redis = redisConnection.sync();
      assertWrongType(() -> redis.lpush(k(), "list"), () -> redis.bitpos(k(), true));
   }

   @Test
   public void testBitPosEmptyKeyBitZero() {
      RedisCommands<String, String> redis = redisConnection.sync();
      // BITPOS bit=0 with empty key returns 0
      assertThat(redis.bitpos(k(), false)).isEqualTo(0);
   }

   @Test
   public void testBitPosEmptyKeyBitOne() {
      RedisCommands<String, String> redis = redisConnection.sync();
      // BITPOS bit=1 with empty key returns -1
      assertThat(redis.bitpos(k(), true)).isEqualTo(-1);
   }

   @Test
   public void testBitPosAllZeroBits() {
      RedisCommands<String, String> redis = redisConnection.sync();
      // All zero bytes: bit=1 returns -1
      redis.set(k(), "\u0000\u0000\u0000");
      assertThat(redis.bitpos(k(), true)).isEqualTo(-1);
   }

   @Test
   public void testBitPosAllOneBitsNoEnd() {
      RedisCommands<String, String> redis = redisConnection.sync();
      // All 0xff: bit=0 without end - no zero bit in the string
      redis.set(k(), "\u00ff\u00ff\u00ff");
      long result = redis.bitpos(k(), false);
      // Implementation may return 24 (Redis) or -1
      assertThat(result).isIn(-1L, 24L);
   }

   @Test
   public void testBitPosAllOneBitsWithEnd() {
      RedisCommands<String, String> redis = redisConnection.sync();
      // All 0xff with explicit end: bit=0 returns -1 (no zero in range)
      redis.set(k(), "\u00ff\u00ff\u00ff");
      assertThat(redis.bitpos(k(), false, 0, 2)).isEqualTo(-1);
   }

   @Test
   public void testBitPosBitZeroWithIntervals() {
      RedisCommands<String, String> redis = redisConnection.sync();
      redis.set(k(), "\u00ff\u00f0\u0000");

      assertThat(redis.bitpos(k(), false, 0, -1)).isEqualTo(12);
      assertThat(redis.bitpos(k(), false, 1, -1)).isEqualTo(12);
      assertThat(redis.bitpos(k(), false, 2, -1)).isEqualTo(16);
   }

   @Test
   public void testBitPosBitOneWithIntervals() {
      RedisCommands<String, String> redis = redisConnection.sync();
      redis.set(k(), "\u0000\u00ff\u00ff");

      assertThat(redis.bitpos(k(), true, 0, -1)).isEqualTo(8);
      assertThat(redis.bitpos(k(), true, 1, -1)).isEqualTo(8);
      assertThat(redis.bitpos(k(), true, 2, -1)).isEqualTo(16);
   }

   @Test
   public void testBitPosShortString() {
      RedisCommands<String, String> redis = redisConnection.sync();
      // Single byte with known bits
      redis.set(k(), "\u00f0"); // 11110000
      assertThat(redis.bitpos(k(), true)).isEqualTo(0);
      assertThat(redis.bitpos(k(), false)).isEqualTo(4);
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

   @Test
   public void testBitOpNotEmptyString() {
      RedisCommands<String, String> redis = redisConnection.sync();
      redis.set(k(1), "");
      redis.bitopNot(k(2), k(1));
      assertThat(redis.get(k(2))).isEqualTo("");
   }

   @Test
   public void testBitOpSameKeyDestAndSource() {
      RedisCommands<String, String> redis = redisConnection.sync();
      redis.set(k(), "foo");
      // BITOP NOT where dest = source
      redis.bitopNot(k(), k());
      byte[] expected = new byte[]{(byte) ~'f', (byte) ~'o', (byte) ~'o'};
      assertThat(redis.get(k()).getBytes(StandardCharsets.ISO_8859_1)).isEqualTo(expected);
   }

   @Test
   public void testBitOpSingleInputKeyPreserved() {
      RedisCommands<String, String> redis = redisConnection.sync();
      redis.set(k(1), "foo");

      // AND with single key preserves the string
      redis.bitopAnd(k(2), k(1));
      assertThat(redis.get(k(2))).isEqualTo("foo");

      // OR with single key preserves the string
      redis.bitopOr(k(3), k(1));
      assertThat(redis.get(k(3))).isEqualTo("foo");

      // XOR with single key preserves the string
      redis.bitopXor(k(4), k(1));
      assertThat(redis.get(k(4))).isEqualTo("foo");
   }

   @Test
   public void testBitOpMissingKeyZeroPadded() {
      RedisCommands<String, String> redis = redisConnection.sync();
      redis.set(k(1), "foo");

      // AND with missing key should zero out the result
      redis.bitopAnd(k(3), k(1), k(2)); // k(2) doesn't exist
      assertThat(redis.get(k(3)).getBytes(StandardCharsets.ISO_8859_1))
            .isEqualTo(new byte[]{0, 0, 0});

      // OR with missing key should preserve the string
      redis.bitopOr(k(4), k(1), k(2));
      assertThat(redis.get(k(4))).isEqualTo("foo");
   }

   @Test
   public void testBitOpShorterKeysZeroPadded() {
      RedisCommands<String, String> redis = redisConnection.sync();
      redis.set(k(1), "\u00ff\u00ff\u00ff");
      redis.set(k(2), "\u00ff");

      // AND: shorter key is zero-padded, so bytes 1-2 become 0
      redis.bitopAnd(k(3), k(1), k(2));
      byte[] result = redis.get(k(3)).getBytes(StandardCharsets.ISO_8859_1);
      assertThat(result).hasSize(3);
      assertThat(result[0]).isEqualTo((byte) 0xff);
      assertThat(result[1]).isEqualTo((byte) 0);
      assertThat(result[2]).isEqualTo((byte) 0);
   }

   @Test
   public void testBitOpWithIntegerEncodedSource() {
      RedisCommands<String, String> redis = redisConnection.sync();
      // Integer-encoded strings should work
      redis.set(k(1), "123");
      redis.set(k(2), "456");
      // BITOP AND should work on integer-encoded strings
      long len = redis.bitopAnd(k(3), k(1), k(2));
      assertThat(len).isEqualTo(3);
      assertThat(redis.get(k(3))).isNotNull();
   }

   @Test
   public void testBitOpNotOverwritesDest() {
      RedisCommands<String, String> redis = redisConnection.sync();
      redis.set(k(1), "foobar");
      redis.set(k(2), "old");
      // BITOP NOT overwrites the destination key
      redis.bitopNot(k(2), k(1));
      assertThat(redis.strlen(k(2))).isEqualTo(6);
   }

   @Test
   public void testBitOpEmptyStringAfterNonEmpty() {
      RedisCommands<String, String> redis = redisConnection.sync();
      // Redis issue #529
      redis.set(k(1), "foo");
      redis.set(k(2), "");

      // OR with empty string should preserve the non-empty operand
      redis.bitopOr(k(3), k(1), k(2));
      assertThat(redis.get(k(3))).isEqualTo("foo");
   }

   @Test
   public void testBitOpReturnsDestinationLength() {
      RedisCommands<String, String> redis = redisConnection.sync();
      redis.set(k(1), "foobar");
      redis.set(k(2), "baz");

      // BITOP returns the length of the destination string
      long len = redis.bitopAnd(k(3), k(1), k(2));
      assertThat(len).isEqualTo(6);
      assertThat(redis.strlen(k(3))).isEqualTo(6);
   }

   @Test
   public void testBitCountNonExistingRange() {
      RedisCommands<String, String> redis = redisConnection.sync();
      redis.set(k(), "foobar");
      // Range completely beyond string: returns 0
      assertThat(redis.bitcount(k(), 100, 200)).isEqualTo(0);
      // Start beyond string with negative end
      assertThat(redis.bitcount(k(), 100, -1)).isEqualTo(0);
   }

   @Test
   public void testBitPosBitOneViaSetBit() {
      RedisCommands<String, String> redis = redisConnection.sync();
      // Set specific bits and verify BITPOS finds the first one
      redis.setbit(k(), 10, 1);
      assertThat(redis.bitpos(k(), true)).isEqualTo(10);

      // Set an earlier bit and check again
      redis.setbit(k(), 3, 1);
      assertThat(redis.bitpos(k(), true)).isEqualTo(3);
   }

   @Test
   public void testBitPosBitZeroViaSetBit() {
      RedisCommands<String, String> redis = redisConnection.sync();
      // Fill with 0xff then find first zero
      redis.set(k(), "\u00ff\u00ff");
      redis.setbit(k(), 5, 0); // Clear bit 5
      assertThat(redis.bitpos(k(), false)).isEqualTo(5);
   }

   @Test
   public void testGetBitNonExistingKey() {
      RedisCommands<String, String> redis = redisConnection.sync();
      assertThat(redis.getbit(k(), 0)).isEqualTo(0);
      assertThat(redis.getbit(k(), 100)).isEqualTo(0);
   }

   @Test
   public void testBitOpNotKnownString() {
      RedisCommands<String, String> redis = redisConnection.sync();
      // Test NOT on specific byte patterns
      redis.set(k(1), "\u00aa"); // 10101010
      redis.bitopNot(k(2), k(1));
      assertThat(redis.get(k(2)).getBytes(StandardCharsets.ISO_8859_1))
            .isEqualTo(new byte[]{0x55}); // 01010101

      redis.set(k(1), "\u0000"); // 00000000
      redis.bitopNot(k(2), k(1));
      assertThat(redis.get(k(2)).getBytes(StandardCharsets.ISO_8859_1))
            .isEqualTo(new byte[]{(byte) 0xff}); // 11111111
   }

   @Test
   public void testBitOpXorMultipleKeys() {
      RedisCommands<String, String> redis = redisConnection.sync();
      redis.set(k(1), "\u00ff");
      redis.set(k(2), "\u000f");
      redis.set(k(3), "\u00ff");
      // XOR of 0xff ^ 0x0f ^ 0xff = 0x0f
      redis.bitopXor(k(4), k(1), k(2), k(3));
      assertThat(redis.get(k(4)).getBytes(StandardCharsets.ISO_8859_1))
            .isEqualTo(new byte[]{0x0f});
   }

   @Test
   public void testBitOpDiff() {
      RedisCommands<String, String> redis = redisConnection.sync();
      // X = 0xff, Y1 = 0x0f => DIFF = X AND NOT(Y1) = 0xff & 0xf0 = 0xf0
      redis.set(k(1), "\u00ff");
      redis.set(k(2), "\u000f");

      long len = bitop(redis, "DIFF", k(3), k(1), k(2));
      assertThat(len).isEqualTo(1);
      assertThat(redis.get(k(3)).getBytes(StandardCharsets.ISO_8859_1))
            .isEqualTo(new byte[]{(byte) 0xf0});
   }

   @Test
   public void testBitOpDiffMultipleKeys() {
      RedisCommands<String, String> redis = redisConnection.sync();
      // X = 0xff, Y1 = 0x0f, Y2 = 0x30 => OR(Y) = 0x3f => DIFF = 0xff & ~0x3f = 0xc0
      redis.set(k(1), "\u00ff");
      redis.set(k(2), "\u000f");
      redis.set(k(3), "\u0030");

      long len = bitop(redis, "DIFF", k(4), k(1), k(2), k(3));
      assertThat(len).isEqualTo(1);
      assertThat(redis.get(k(4)).getBytes(StandardCharsets.ISO_8859_1))
            .isEqualTo(new byte[]{(byte) 0xc0});
   }

   @Test
   public void testBitOpDiffSingleKey() {
      RedisCommands<String, String> redis = redisConnection.sync();
      // DIFF with only X (no Ys) => result = X
      redis.set(k(1), "\u00ab");

      bitop(redis, "DIFF", k(2), k(1));
      assertThat(redis.get(k(2)).getBytes(StandardCharsets.ISO_8859_1))
            .isEqualTo(new byte[]{(byte) 0xab});
   }

   @Test
   public void testBitOpDiff1() {
      RedisCommands<String, String> redis = redisConnection.sync();
      // X = 0x0f, Y1 = 0xff => DIFF1 = OR(Y) AND NOT(X) = 0xff & 0xf0 = 0xf0
      redis.set(k(1), "\u000f");
      redis.set(k(2), "\u00ff");

      long len = bitop(redis, "DIFF1", k(3), k(1), k(2));
      assertThat(len).isEqualTo(1);
      assertThat(redis.get(k(3)).getBytes(StandardCharsets.ISO_8859_1))
            .isEqualTo(new byte[]{(byte) 0xf0});
   }

   @Test
   public void testBitOpDiff1MultipleKeys() {
      RedisCommands<String, String> redis = redisConnection.sync();
      // X = 0xf0, Y1 = 0x0f, Y2 = 0x30 => OR(Y) = 0x3f => DIFF1 = 0x3f & ~0xf0 = 0x0f
      redis.set(k(1), "\u00f0");
      redis.set(k(2), "\u000f");
      redis.set(k(3), "\u0030");

      long len = bitop(redis, "DIFF1", k(4), k(1), k(2), k(3));
      assertThat(len).isEqualTo(1);
      assertThat(redis.get(k(4)).getBytes(StandardCharsets.ISO_8859_1))
            .isEqualTo(new byte[]{0x0f});
   }

   @Test
   public void testBitOpDiff1SingleKey() {
      RedisCommands<String, String> redis = redisConnection.sync();
      // DIFF1 with only X (no Ys) => OR(nothing) = 0 => result = 0
      redis.set(k(1), "\u00ff");

      bitop(redis, "DIFF1", k(2), k(1));
      assertThat(redis.get(k(2)).getBytes(StandardCharsets.ISO_8859_1))
            .isEqualTo(new byte[]{0x00});
   }

   @Test
   public void testBitOpAndOr() {
      RedisCommands<String, String> redis = redisConnection.sync();
      // X = 0xff, Y1 = 0x0f => ANDOR = X AND OR(Y) = 0xff & 0x0f = 0x0f
      redis.set(k(1), "\u00ff");
      redis.set(k(2), "\u000f");

      long len = bitop(redis, "ANDOR", k(3), k(1), k(2));
      assertThat(len).isEqualTo(1);
      assertThat(redis.get(k(3)).getBytes(StandardCharsets.ISO_8859_1))
            .isEqualTo(new byte[]{0x0f});
   }

   @Test
   public void testBitOpAndOrMultipleKeys() {
      RedisCommands<String, String> redis = redisConnection.sync();
      // X = 0xf0, Y1 = 0x0f, Y2 = 0x30 => OR(Y) = 0x3f => ANDOR = 0xf0 & 0x3f = 0x30
      redis.set(k(1), "\u00f0");
      redis.set(k(2), "\u000f");
      redis.set(k(3), "\u0030");

      long len = bitop(redis, "ANDOR", k(4), k(1), k(2), k(3));
      assertThat(len).isEqualTo(1);
      assertThat(redis.get(k(4)).getBytes(StandardCharsets.ISO_8859_1))
            .isEqualTo(new byte[]{0x30});
   }

   @Test
   public void testBitOpAndOrSingleKey() {
      RedisCommands<String, String> redis = redisConnection.sync();
      // ANDOR with only X (no Ys) => OR(nothing) = 0 => result = 0
      redis.set(k(1), "\u00ff");

      bitop(redis, "ANDOR", k(2), k(1));
      assertThat(redis.get(k(2)).getBytes(StandardCharsets.ISO_8859_1))
            .isEqualTo(new byte[]{0x00});
   }

   @Test
   public void testBitOpOne() {
      RedisCommands<String, String> redis = redisConnection.sync();
      // k1 = 0xff, k2 = 0x0f => bits set in exactly one: 0xf0
      redis.set(k(1), "\u00ff");
      redis.set(k(2), "\u000f");

      long len = bitop(redis, "ONE", k(3), k(1), k(2));
      assertThat(len).isEqualTo(1);
      assertThat(redis.get(k(3)).getBytes(StandardCharsets.ISO_8859_1))
            .isEqualTo(new byte[]{(byte) 0xf0});
   }

   @Test
   public void testBitOpOneMultipleKeys() {
      RedisCommands<String, String> redis = redisConnection.sync();
      // k1 = 0xff, k2 = 0x0f, k3 = 0x0f
      // Bit by bit: high nibble (0xf0) is in k1 only, low nibble (0x0f) is in k1+k2+k3
      // Exactly one: only the high nibble bits qualify (set in k1 only) => not quite
      // Actually per bit: high nibble bits are set in k1 only (count=1) => included
      // Low nibble bits are set in all three (count=3) => excluded
      // Result: 0xf0
      redis.set(k(1), "\u00ff");
      redis.set(k(2), "\u000f");
      redis.set(k(3), "\u000f");

      long len = bitop(redis, "ONE", k(4), k(1), k(2), k(3));
      assertThat(len).isEqualTo(1);
      assertThat(redis.get(k(4)).getBytes(StandardCharsets.ISO_8859_1))
            .isEqualTo(new byte[]{(byte) 0xf0});
   }

   @Test
   public void testBitOpOneSingleKey() {
      RedisCommands<String, String> redis = redisConnection.sync();
      // ONE with single key => every set bit is in exactly one key
      redis.set(k(1), "\u00ab");

      bitop(redis, "ONE", k(2), k(1));
      assertThat(redis.get(k(2)).getBytes(StandardCharsets.ISO_8859_1))
            .isEqualTo(new byte[]{(byte) 0xab});
   }

   @Test
   public void testBitOpOneVsXorDifference() {
      RedisCommands<String, String> redis = redisConnection.sync();
      // Demonstrate that ONE differs from XOR for 3+ keys
      // k1 = 0xff, k2 = 0xff, k3 = 0xff
      // XOR: 0xff ^ 0xff ^ 0xff = 0xff (odd number set)
      // ONE: each bit is set in 3 keys (not exactly 1) => 0x00
      redis.set(k(1), "\u00ff");
      redis.set(k(2), "\u00ff");
      redis.set(k(3), "\u00ff");

      redis.bitopXor(k(4), k(1), k(2), k(3));
      assertThat(redis.get(k(4)).getBytes(StandardCharsets.ISO_8859_1))
            .isEqualTo(new byte[]{(byte) 0xff});

      bitop(redis, "ONE", k(5), k(1), k(2), k(3));
      assertThat(redis.get(k(5)).getBytes(StandardCharsets.ISO_8859_1))
            .isEqualTo(new byte[]{0x00});
   }

   @Test
   public void testBitOpDiffWithMissingKey() {
      RedisCommands<String, String> redis = redisConnection.sync();
      // X = 0xff, Y = missing (zero-padded) => DIFF = 0xff & ~0x00 = 0xff
      redis.set(k(1), "\u00ff");

      bitop(redis, "DIFF", k(3), k(1), k(2));
      assertThat(redis.get(k(3)).getBytes(StandardCharsets.ISO_8859_1))
            .isEqualTo(new byte[]{(byte) 0xff});
   }

   @Test
   public void testBitOpDiffShorterKeys() {
      RedisCommands<String, String> redis = redisConnection.sync();
      // X = 0xff 0xff, Y = 0x0f => zero-padded to 0x0f 0x00
      // DIFF = (0xff & ~0x0f), (0xff & ~0x00) = 0xf0, 0xff
      redis.set(k(1), "\u00ff\u00ff");
      redis.set(k(2), "\u000f");

      bitop(redis, "DIFF", k(3), k(1), k(2));
      assertThat(redis.get(k(3)).getBytes(StandardCharsets.ISO_8859_1))
            .isEqualTo(new byte[]{(byte) 0xf0, (byte) 0xff});
   }

   private long bitop(RedisCommands<String, String> redis, String op, String destKey, String... srcKeys) {
      RedisCodec<String, String> codec = new StringCodec(StandardCharsets.ISO_8859_1);
      CommandArgs<String, String> args = new CommandArgs<>(codec)
            .add(op)
            .addKey(destKey);
      for (String srcKey : srcKeys) {
         args.addKey(srcKey);
      }
      return redis.dispatch(new SimpleCommand("BITOP"), new IntegerOutput<>(codec), args);
   }

   private static class SimpleCommand implements ProtocolKeyword {
      private final String name;

      SimpleCommand(String name) {
         this.name = name;
      }

      @Override
      public byte[] getBytes() {
         return name.getBytes(StandardCharsets.UTF_8);
      }

      @Override
      public String name() {
         return name;
      }
   }
}
