package org.infinispan.server.resp.commands.string;

/**
 * XXH3-64 hash function implementation.
 * <p>
 * This is a pure Java implementation of the XXH3-64 algorithm, compatible with
 * the hash digests used by Redis 8.4.0+ for the DIGEST and DELEX commands.
 *
 * @since 16.2
 */
public final class XXH3 {

   private static final char[] HEX_CHARS = "0123456789abcdef".toCharArray();

   private static final long PRIME32_1 = 0x9E3779B1L;
   private static final long PRIME32_2 = 0x85EBCA77L;
   private static final long PRIME32_3 = 0xC2B2AE3DL;
   private static final long PRIME64_1 = 0x9E3779B185EBCA87L;
   private static final long PRIME64_2 = 0xC2B2AE3D27D4EB4FL;
   private static final long PRIME64_3 = 0x165667B19E3779F9L;
   private static final long PRIME64_4 = 0x85EBCA77C2B2AE63L;
   private static final long PRIME64_5 = 0x27D4EB2F165667C5L;
   private static final long PRIME_MX1 = 0x165667919E3779F9L;
   private static final long PRIME_MX2 = 0x9FB21C651E98DF25L;

   // The default 192-byte secret (kSecret in xxhash.h)
   private static final byte[] SECRET = {
         (byte) 0xb8, (byte) 0xfe, (byte) 0x6c, (byte) 0x39, (byte) 0x23, (byte) 0xa4, (byte) 0x4b, (byte) 0xbe,
         (byte) 0x7c, (byte) 0x01, (byte) 0x81, (byte) 0x2c, (byte) 0xf7, (byte) 0x21, (byte) 0xad, (byte) 0x1c,
         (byte) 0xde, (byte) 0xd4, (byte) 0x6d, (byte) 0xe9, (byte) 0x83, (byte) 0x90, (byte) 0x97, (byte) 0xdb,
         (byte) 0x72, (byte) 0x40, (byte) 0xa4, (byte) 0xa4, (byte) 0xb7, (byte) 0xb3, (byte) 0x67, (byte) 0x1f,
         (byte) 0xcb, (byte) 0x79, (byte) 0xe6, (byte) 0x4e, (byte) 0xcc, (byte) 0xc0, (byte) 0xe5, (byte) 0x78,
         (byte) 0x82, (byte) 0x5a, (byte) 0xd0, (byte) 0x7d, (byte) 0xcc, (byte) 0xff, (byte) 0x72, (byte) 0x21,
         (byte) 0xb8, (byte) 0x08, (byte) 0x46, (byte) 0x74, (byte) 0xf7, (byte) 0x43, (byte) 0x24, (byte) 0x8e,
         (byte) 0xe0, (byte) 0x35, (byte) 0x90, (byte) 0xe6, (byte) 0x81, (byte) 0x3a, (byte) 0x26, (byte) 0x4c,
         (byte) 0x3c, (byte) 0x28, (byte) 0x52, (byte) 0xbb, (byte) 0x91, (byte) 0xc3, (byte) 0x00, (byte) 0xcb,
         (byte) 0x88, (byte) 0xd0, (byte) 0x65, (byte) 0x8b, (byte) 0x1b, (byte) 0x53, (byte) 0x2e, (byte) 0xa3,
         (byte) 0x71, (byte) 0x64, (byte) 0x48, (byte) 0x97, (byte) 0xa2, (byte) 0x0d, (byte) 0xf9, (byte) 0x4e,
         (byte) 0x38, (byte) 0x19, (byte) 0xef, (byte) 0x46, (byte) 0xa9, (byte) 0xde, (byte) 0xac, (byte) 0xd8,
         (byte) 0xa8, (byte) 0xfa, (byte) 0x76, (byte) 0x3f, (byte) 0xe3, (byte) 0x9c, (byte) 0x34, (byte) 0x3f,
         (byte) 0xf9, (byte) 0xdc, (byte) 0xbb, (byte) 0xc7, (byte) 0xc7, (byte) 0x0b, (byte) 0x4f, (byte) 0x1d,
         (byte) 0x8a, (byte) 0x51, (byte) 0xe0, (byte) 0x4b, (byte) 0xcd, (byte) 0xb4, (byte) 0x59, (byte) 0x31,
         (byte) 0xc8, (byte) 0x9f, (byte) 0x7e, (byte) 0xc9, (byte) 0xd9, (byte) 0x78, (byte) 0x73, (byte) 0x64,
         (byte) 0xea, (byte) 0xc5, (byte) 0xac, (byte) 0x83, (byte) 0x34, (byte) 0xd3, (byte) 0xeb, (byte) 0xc3,
         (byte) 0xc5, (byte) 0x81, (byte) 0xa0, (byte) 0xff, (byte) 0xfa, (byte) 0x13, (byte) 0x63, (byte) 0xeb,
         (byte) 0x17, (byte) 0x0d, (byte) 0xdd, (byte) 0x51, (byte) 0xb7, (byte) 0xf0, (byte) 0xda, (byte) 0x49,
         (byte) 0xd3, (byte) 0x16, (byte) 0x55, (byte) 0x26, (byte) 0x29, (byte) 0xd4, (byte) 0x68, (byte) 0x9e,
         (byte) 0x2b, (byte) 0x16, (byte) 0xbe, (byte) 0x58, (byte) 0x7d, (byte) 0x47, (byte) 0xa1, (byte) 0xfc,
         (byte) 0x8f, (byte) 0xf8, (byte) 0xb8, (byte) 0xd1, (byte) 0x7a, (byte) 0xd0, (byte) 0x31, (byte) 0xce,
         (byte) 0x45, (byte) 0xcb, (byte) 0x3a, (byte) 0x8f, (byte) 0x95, (byte) 0x16, (byte) 0x04, (byte) 0x28,
         (byte) 0xaf, (byte) 0xd7, (byte) 0xfb, (byte) 0xca, (byte) 0xbb, (byte) 0x4b, (byte) 0x40, (byte) 0x7e
   };

   private XXH3() {
   }

   /**
    * Computes the XXH3-64 hash of the input bytes.
    *
    * @param input the byte array to hash
    * @return the 64-bit hash value
    */
   public static long hash64(byte[] input) {
      int len = input.length;

      if (len <= 16) {
         return len0to16(input, len);
      } else if (len <= 128) {
         return len17to128(input, len);
      } else if (len <= 240) {
         return len129to240(input, len);
      } else {
         return hashLong(input, len);
      }
   }

   /**
    * Computes the XXH3-64 hash and returns it as a 16-character hex string.
    *
    * @param input the byte array to hash
    * @return the hash as a lowercase hexadecimal string
    */
   public static String hashToHex(byte[] input) {
      return toHexString(hash64(input));
   }

   /**
    * Converts a long value to a 16-character lowercase hexadecimal string.
    */
   public static String toHexString(long value) {
      char[] hex = new char[16];
      for (int i = 15; i >= 0; i--) {
         hex[i] = HEX_CHARS[(int) (value & 0xF)];
         value >>>= 4;
      }
      return new String(hex);
   }

   /**
    * Parses a 16-character hexadecimal string to a long value.
    *
    * @param hex the hexadecimal string
    * @return the parsed long value, or -1 if invalid
    */
   public static long parseHexDigest(String hex) {
      if (hex == null || hex.length() != 16) {
         return -1;
      }
      try {
         return Long.parseUnsignedLong(hex, 16);
      } catch (NumberFormatException e) {
         return -1;
      }
   }

   private static long len0to16(byte[] input, int len) {
      if (len > 8) {
         long inputLo = readLE64(input, 0) ^ (readLE64(SECRET, 24) ^ readLE64(SECRET, 32));
         long inputHi = readLE64(input, len - 8) ^ (readLE64(SECRET, 40) ^ readLE64(SECRET, 48));
         long acc = len + Long.reverseBytes(inputLo) + inputHi + mul128Fold64(inputLo, inputHi);
         return avalanche(acc);
      } else if (len >= 4) {
         long input1 = readLE32(input, 0);
         long input2 = readLE32(input, len - 4);
         long keyed = (input2 + (input1 << 32)) ^ (readLE64(SECRET, 8) ^ readLE64(SECRET, 16));
         return rrmxmx(keyed, len);
      } else if (len > 0) {
         int c1 = input[0] & 0xFF;
         int c2 = input[len >> 1] & 0xFF;
         int c3 = input[len - 1] & 0xFF;
         long combined = Integer.toUnsignedLong((c1 << 16) | (c2 << 24) | c3 | (len << 8));
         return xxh64Avalanche(combined ^ (readLE32(SECRET, 0) ^ readLE32(SECRET, 4)));
      }
      return xxh64Avalanche(readLE64(SECRET, 56) ^ readLE64(SECRET, 64));
   }

   private static long len17to128(byte[] input, int len) {
      long acc = len * PRIME64_1;
      if (len > 32) {
         if (len > 64) {
            if (len > 96) {
               acc += mix16B(input, 48, 96);
               acc += mix16B(input, len - 64, 112);
            }
            acc += mix16B(input, 32, 64);
            acc += mix16B(input, len - 48, 80);
         }
         acc += mix16B(input, 16, 32);
         acc += mix16B(input, len - 32, 48);
      }
      acc += mix16B(input, 0, 0);
      acc += mix16B(input, len - 16, 16);
      return avalanche(acc);
   }

   private static long len129to240(byte[] input, int len) {
      long acc = len * PRIME64_1;
      for (int i = 0; i < 8; i++) {
         acc += mix16B(input, 16 * i, 16 * i);
      }
      acc = avalanche(acc);
      int nbRounds = len / 16;
      for (int i = 8; i < nbRounds; i++) {
         acc += mix16B(input, 16 * i, 16 * (i - 8) + 3);
      }
      // XXH3_SECRET_SIZE_MIN - XXH3_MIDSIZE_LASTOFFSET
      acc += mix16B(input, len - 16, 136 - 17);
      return avalanche(acc);
   }

   private static long hashLong(byte[] input, int len) {
      long[] acc = {PRIME32_3, PRIME64_1, PRIME64_2, PRIME64_3, PRIME64_4, PRIME32_2, PRIME64_5, PRIME32_1};
      // Blocks of 16 stripes of 64 bytes, scrambling the accumulators after each block
      int nbBlocks = (len - 1) / 1024;
      for (int n = 0; n < nbBlocks; n++) {
         for (int s = 0; s < 16; s++) {
            accumulate512(acc, input, n * 1024 + s * 64, s * 8);
         }
         scramble(acc);
      }
      // The stripes of the last, partial block
      int nbStripes = ((len - 1) - nbBlocks * 1024) / 64;
      for (int s = 0; s < nbStripes; s++) {
         accumulate512(acc, input, nbBlocks * 1024 + s * 64, s * 8);
      }
      // The last 64 bytes, always
      accumulate512(acc, input, len - 64, 192 - 64 - 7);
      long result = len * PRIME64_1;
      for (int i = 0; i < 4; i++) {
         result += mul128Fold64(acc[2 * i] ^ readLE64(SECRET, 11 + 16 * i), acc[2 * i + 1] ^ readLE64(SECRET, 11 + 16 * i + 8));
      }
      return avalanche(result);
   }

   private static void accumulate512(long[] acc, byte[] input, int inputOffset, int secretOffset) {
      for (int i = 0; i < 8; i++) {
         long dataVal = readLE64(input, inputOffset + 8 * i);
         long dataKey = dataVal ^ readLE64(SECRET, secretOffset + 8 * i);
         acc[i ^ 1] += dataVal;
         acc[i] += (dataKey & 0xFFFFFFFFL) * (dataKey >>> 32);
      }
   }

   private static void scramble(long[] acc) {
      for (int i = 0; i < 8; i++) {
         long a = acc[i];
         a ^= a >>> 47;
         a ^= readLE64(SECRET, 128 + 8 * i);
         a *= PRIME32_1;
         acc[i] = a;
      }
   }

   private static long mix16B(byte[] input, int inputOffset, int secretOffset) {
      return mul128Fold64(readLE64(input, inputOffset) ^ readLE64(SECRET, secretOffset),
            readLE64(input, inputOffset + 8) ^ readLE64(SECRET, secretOffset + 8));
   }

   /**
    * Multiplies two 64-bit values into 128 bits and XORs the two halves.
    * Math.unsignedMultiplyHigh needs Java 18, so this corrects the signed high half instead.
    */
   private static long mul128Fold64(long a, long b) {
      long high = Math.multiplyHigh(a, b) + ((a >> 63) & b) + ((b >> 63) & a);
      return (a * b) ^ high;
   }

   private static long avalanche(long h) {
      h ^= h >>> 37;
      h *= PRIME_MX1;
      return h ^ (h >>> 32);
   }

   private static long rrmxmx(long h, int len) {
      h ^= Long.rotateLeft(h, 49) ^ Long.rotateLeft(h, 24);
      h *= PRIME_MX2;
      h ^= (h >>> 35) + len;
      h *= PRIME_MX2;
      return h ^ (h >>> 28);
   }

   private static long xxh64Avalanche(long h) {
      h ^= h >>> 33;
      h *= PRIME64_2;
      h ^= h >>> 29;
      h *= PRIME64_3;
      return h ^ (h >>> 32);
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

   private static long readLE32(byte[] buf, int offset) {
      return (buf[offset] & 0xFFL)
            | ((buf[offset + 1] & 0xFFL) << 8)
            | ((buf[offset + 2] & 0xFFL) << 16)
            | ((buf[offset + 3] & 0xFFL) << 24);
   }
}
