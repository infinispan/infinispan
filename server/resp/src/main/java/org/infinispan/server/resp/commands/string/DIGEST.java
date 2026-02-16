package org.infinispan.server.resp.commands.string;

import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.server.resp.AclCategory;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;

import io.netty.channel.ChannelHandlerContext;

/**
 * DIGEST key
 * <p>
 * Returns the XXH3 hash digest of a string value as a hexadecimal string.
 *
 * @see <a href="https://redis.io/commands/digest/">DIGEST</a>
 * @since 16.2
 */
public class DIGEST extends RespCommand implements Resp3Command {

   private static final char[] HEX_CHARS = "0123456789abcdef".toCharArray();

   public DIGEST() {
      super(2, 1, 1, 1, AclCategory.READ.mask() | AclCategory.STRING.mask() | AclCategory.FAST.mask());
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler,
                                                      ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {
      byte[] keyBytes = arguments.get(0);

      return handler.stageToReturn(
            handler.cache().getAsync(keyBytes).thenApply(value -> {
               if (value == null) {
                  return null;
               }
               long hash = xxh3_64(value);
               return toHexString(hash);
            }),
            ctx,
            (res, writer) -> writer.string(res)
      );
   }

   /**
    * Converts a long value to a 16-character lowercase hexadecimal string.
    */
   private static String toHexString(long value) {
      char[] hex = new char[16];
      for (int i = 15; i >= 0; i--) {
         hex[i] = HEX_CHARS[(int) (value & 0xF)];
         value >>>= 4;
      }
      return new String(hex);
   }

   // XXH3-64 implementation constants
   private static final long XXH_PRIME64_1 = 0x9E3779B185EBCA87L;
   private static final long XXH_PRIME64_2 = 0xC2B2AE3D27D4EB4FL;
   private static final long XXH_PRIME64_3 = 0x165667B19E3779F9L;
   private static final long XXH_PRIME64_4 = 0x85EBCA77C2B2AE63L;
   private static final long XXH_PRIME64_5 = 0x27D4EB2F165667C5L;

   // XXH3 secret (first 64 bytes of the default secret)
   private static final long[] XXH3_SECRET = {
         0xbe4ba423396cfeb8L, 0x1cad21f72c81017cL,
         0xdb979083e96dd4deL, 0x1f67b3b7a4a44072L,
         0x78e5c0cc4ee679cbL, 0x2172ffcc7dd05a82L,
         0x8e2443f7744608b8L, 0x4c263a81e69035e0L
   };

   /**
    * XXH3-64 hash function implementation.
    * This is a simplified implementation suitable for small to medium inputs.
    */
   private static long xxh3_64(byte[] input) {
      int len = input.length;

      if (len <= 16) {
         return xxh3_len_0to16(input, len);
      } else if (len <= 128) {
         return xxh3_len_17to128(input, len);
      } else if (len <= 240) {
         return xxh3_len_129to240(input, len);
      } else {
         return xxh3_hashLong(input, len);
      }
   }

   private static long xxh3_len_0to16(byte[] input, int len) {
      if (len > 8) {
         long lo = readLE64(input, 0) ^ (XXH3_SECRET[0] ^ XXH3_SECRET[1]);
         long hi = readLE64(input, len - 8) ^ (XXH3_SECRET[1] ^ XXH3_SECRET[2]);
         long acc = len + Long.reverseBytes(lo) + hi + xxh3_mul128_fold64(lo, hi);
         return xxh3_avalanche(acc);
      } else if (len >= 4) {
         long input1 = Integer.toUnsignedLong(readLE32(input, 0));
         long input2 = Integer.toUnsignedLong(readLE32(input, len - 4));
         long keyed = ((input2 + (input1 << 32)) ^ (XXH3_SECRET[0] ^ XXH3_SECRET[1]));
         return xxh3_rrmxmx(keyed, len);
      } else if (len > 0) {
         int c1 = input[0] & 0xFF;
         int c2 = input[len >> 1] & 0xFF;
         int c3 = input[len - 1] & 0xFF;
         int combined = (c1 << 16) | (c2 << 24) | c3 | (len << 8);
         long keyed = (Integer.toUnsignedLong(combined)) ^ (XXH3_SECRET[0] ^ XXH3_SECRET[1]);
         return xxh64_avalanche(keyed);
      }
      return xxh64_avalanche(XXH3_SECRET[0] ^ XXH3_SECRET[1]);
   }

   private static long xxh3_len_17to128(byte[] input, int len) {
      long acc = len * XXH_PRIME64_1;
      int nbRounds = ((len - 1) / 32);
      for (int i = 0; i < nbRounds; i++) {
         acc += xxh3_mix16B(input, i * 32, i * 2);
         acc += xxh3_mix16B(input, len - 16 - i * 32, i * 2 + 1);
      }
      if ((len & 31) > 16) {
         acc += xxh3_mix16B(input, (nbRounds * 32), nbRounds * 2);
         acc += xxh3_mix16B(input, len - 16, nbRounds * 2 + 1);
      } else {
         acc += xxh3_mix16B(input, len - 32, nbRounds * 2);
         acc += xxh3_mix16B(input, len - 16, nbRounds * 2 + 1);
      }
      return xxh3_avalanche(acc);
   }

   private static long xxh3_len_129to240(byte[] input, int len) {
      long acc = len * XXH_PRIME64_1;
      int nbRounds = len / 16;
      for (int i = 0; i < 8; i++) {
         acc += xxh3_mix16B(input, i * 16, i);
      }
      acc = xxh3_avalanche(acc);
      for (int i = 8; i < nbRounds; i++) {
         acc += xxh3_mix16B(input, i * 16, i - 8);
      }
      acc += xxh3_mix16B(input, len - 16, (nbRounds - 8) % 8);
      return xxh3_avalanche(acc);
   }

   private static long xxh3_hashLong(byte[] input, int len) {
      // Simplified long input handling using stripe processing
      long acc0 = XXH_PRIME64_1;
      long acc1 = XXH_PRIME64_2;
      long acc2 = XXH_PRIME64_3;
      long acc3 = XXH_PRIME64_4;
      long acc4 = XXH_PRIME64_5;
      long acc5 = XXH_PRIME64_1 ^ XXH_PRIME64_2;
      long acc6 = XXH_PRIME64_2 ^ XXH_PRIME64_3;
      long acc7 = XXH_PRIME64_3 ^ XXH_PRIME64_4;

      int stripes = (len - 1) / 64;
      for (int s = 0; s < stripes; s++) {
         int offset = s * 64;
         acc0 = xxh3_accumulate_lane(acc0, input, offset, 0);
         acc1 = xxh3_accumulate_lane(acc1, input, offset + 8, 1);
         acc2 = xxh3_accumulate_lane(acc2, input, offset + 16, 2);
         acc3 = xxh3_accumulate_lane(acc3, input, offset + 24, 3);
         acc4 = xxh3_accumulate_lane(acc4, input, offset + 32, 4 % 8);
         acc5 = xxh3_accumulate_lane(acc5, input, offset + 40, 5 % 8);
         acc6 = xxh3_accumulate_lane(acc6, input, offset + 48, 6 % 8);
         acc7 = xxh3_accumulate_lane(acc7, input, offset + 56, 7 % 8);
      }

      // Process remaining bytes
      int remaining = len - stripes * 64;
      if (remaining > 0) {
         int lastOffset = len - 64;
         acc0 = xxh3_accumulate_lane(acc0, input, lastOffset, 0);
         acc1 = xxh3_accumulate_lane(acc1, input, lastOffset + 8, 1);
         acc2 = xxh3_accumulate_lane(acc2, input, lastOffset + 16, 2);
         acc3 = xxh3_accumulate_lane(acc3, input, lastOffset + 24, 3);
         acc4 = xxh3_accumulate_lane(acc4, input, lastOffset + 32, 4 % 8);
         acc5 = xxh3_accumulate_lane(acc5, input, lastOffset + 40, 5 % 8);
         acc6 = xxh3_accumulate_lane(acc6, input, lastOffset + 48, 6 % 8);
         acc7 = xxh3_accumulate_lane(acc7, input, lastOffset + 56, 7 % 8);
      }

      long result = len * XXH_PRIME64_1;
      result += xxh3_mix_acc(acc0, 0);
      result += xxh3_mix_acc(acc1, 1);
      result += xxh3_mix_acc(acc2, 2);
      result += xxh3_mix_acc(acc3, 3);
      result += xxh3_mix_acc(acc4, 4);
      result += xxh3_mix_acc(acc5, 5);
      result += xxh3_mix_acc(acc6, 6);
      result += xxh3_mix_acc(acc7, 7);

      return xxh3_avalanche(result);
   }

   private static long xxh3_accumulate_lane(long acc, byte[] input, int offset, int lane) {
      long dataVal = readLE64(input, offset);
      long dataKey = dataVal ^ XXH3_SECRET[lane % 8];
      acc += dataVal + xxh3_mul128_fold64(dataKey & 0xFFFFFFFFL, dataKey >>> 32);
      return acc;
   }

   private static long xxh3_mix_acc(long acc, int lane) {
      return (acc ^ (acc >>> 47) ^ XXH3_SECRET[lane % 8]) * XXH_PRIME64_1;
   }

   private static long xxh3_mix16B(byte[] input, int offset, int secretIndex) {
      long lo = readLE64(input, offset);
      long hi = readLE64(input, offset + 8);
      return xxh3_mul128_fold64(
            lo ^ XXH3_SECRET[secretIndex % 8],
            hi ^ XXH3_SECRET[(secretIndex + 1) % 8]
      );
   }

   private static long xxh3_mul128_fold64(long lo, long hi) {
      // Multiply two 64-bit values and fold the 128-bit result to 64 bits
      long loXlo = (lo & 0xFFFFFFFFL) * (hi & 0xFFFFFFFFL);
      long hiXlo = (lo >>> 32) * (hi & 0xFFFFFFFFL);
      long loXhi = (lo & 0xFFFFFFFFL) * (hi >>> 32);
      long hiXhi = (lo >>> 32) * (hi >>> 32);
      long cross = (loXlo >>> 32) + (hiXlo & 0xFFFFFFFFL) + loXhi;
      long upper = (hiXlo >>> 32) + (cross >>> 32) + hiXhi;
      long lower = (cross << 32) | (loXlo & 0xFFFFFFFFL);
      return lower ^ upper;
   }

   private static long xxh3_avalanche(long h64) {
      h64 ^= h64 >>> 37;
      h64 *= 0x165667919E3779F9L;
      h64 ^= h64 >>> 32;
      return h64;
   }

   private static long xxh3_rrmxmx(long h64, int len) {
      h64 ^= Long.rotateLeft(h64, 49) ^ Long.rotateLeft(h64, 24);
      h64 *= 0x9FB21C651E98DF25L;
      h64 ^= (h64 >>> 35) + len;
      h64 *= 0x9FB21C651E98DF25L;
      return h64 ^ (h64 >>> 28);
   }

   private static long xxh64_avalanche(long h64) {
      h64 ^= h64 >>> 33;
      h64 *= XXH_PRIME64_2;
      h64 ^= h64 >>> 29;
      h64 *= XXH_PRIME64_3;
      h64 ^= h64 >>> 32;
      return h64;
   }

   private static long readLE64(byte[] buf, int offset) {
      return (buf[offset] & 0xFFL)
            | ((buf[offset + 1] & 0xFFL) << 8)
            | ((buf[offset + 2] & 0xFFL) << 16)
            | ((buf[offset + 3] & 0xFFL) << 24)
            | ((buf[offset + 4] & 0xFFL) << 32)
            | ((buf[offset + 5] & 0xFFL) << 40)
            | ((buf[offset + 6] & 0xFFL) << 48)
            | ((buf[offset + 7] & 0xFFL) << 56);
   }

   private static int readLE32(byte[] buf, int offset) {
      return (buf[offset] & 0xFF)
            | ((buf[offset + 1] & 0xFF) << 8)
            | ((buf[offset + 2] & 0xFF) << 16)
            | ((buf[offset + 3] & 0xFF) << 24);
   }
}
