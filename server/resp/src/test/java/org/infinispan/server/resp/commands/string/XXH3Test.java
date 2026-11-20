package org.infinispan.server.resp.commands.string;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import org.testng.annotations.Test;

/**
 * Checks {@link XXH3} against XXH3-64 (seed 0) reference values, covering every input length range: 0-16, 17-128,
 * 129-240 and longer inputs, which are processed in 1024-byte blocks.
 */
@Test(groups = "unit", testName = "server.resp.commands.string.XXH3Test")
public class XXH3Test {

   /**
    * The XXH3_64bits values from xxHash's own sanity check, for prefixes of the buffer
    * built by {@link #sanityBuffer(int)}.
    */
   @Test
   public void testOfficialVectors() {
      int[] lengths = {0, 1, 6, 12, 24, 48, 80, 195, 403, 512, 2048, 2240, 2367};
      long[] expected = {
            0x2D06800538D394C2L, 0xC44BDFF4074EECDBL, 0x27B56A84CD2D7325L, 0xA713DAF0DFBB77E7L,
            0xA3FE70BF9D3510EBL, 0x397DA259ECBA1F11L, 0xBCDEFBBB2C47C90AL, 0xCD94217EE362EC3AL,
            0xCDEB804D65C6DEA4L, 0x617E49599013CB6BL, 0xDD59E2C3A5F038E0L, 0x6E73A90539CF2948L,
            0xCB37AEB9E5D361EDL
      };
      byte[] buffer = sanityBuffer(2367);
      for (int i = 0; i < lengths.length; i++) {
         int len = lengths[i];
         assertThat(XXH3.hash64(Arrays.copyOf(buffer, len)))
               .as("length %d", len)
               .isEqualTo(expected[i]);
      }
   }

   /**
    * Bytes with the high bit set catch sign-extension mistakes when reading the input. The lengths are either side
    * of each range boundary.
    */
   @Test
   public void testHighBitBytesAtRangeBoundaries() {
      int[] lengths = {3, 4, 8, 9, 16, 17, 128, 129, 240, 241, 1024, 1025};
      long[] expected = {
            0xa79db4af6ec707daL, 0xcd6b1c920d3f662cL, 0x5111c7e47d784413L, 0x9d13a13de61230f2L,
            0x722a202f07a4b313L, 0xe618c2238a349e4cL, 0xda3383e7b4ff3ccdL, 0xf9581a2eaa508d4eL,
            0xd0b19a475a28a93cL, 0xaaa20fe63bbec0a5L, 0x91a9afd3ad9eb82eL, 0xa1b6201683ab7deeL
      };
      for (int i = 0; i < lengths.length; i++) {
         byte[] input = new byte[lengths[i]];
         Arrays.fill(input, (byte) 0xFF);
         assertThat(XXH3.hash64(input))
               .as("length %d", lengths[i])
               .isEqualTo(expected[i]);
      }
   }

   /**
    * The test vector from redis/redis#14496; its digest also checks that leading zeros are kept.
    */
   @Test
   public void testRedisVector() {
      byte[] value = "v8lf0c11xh8ymlqztfd3eeq16kfn4sspw7fqmnuuq3k3t75em5wdizgcdw7uc26nnf961u2jkfzkjytls2kwlj7626sd"
            .getBytes(StandardCharsets.US_ASCII);
      assertThat(XXH3.hashToHex(value)).isEqualTo("00006c38adf31777");
   }

   /**
    * The input generator of xxHash's sanity check.
    */
   private static byte[] sanityBuffer(int length) {
      byte[] buffer = new byte[length];
      long byteGen = 2654435761L;           // PRIME32
      for (int i = 0; i < length; i++) {
         buffer[i] = (byte) (byteGen >>> 56);
         byteGen *= 0x9E3779B185EBCA8DL;    // PRIME64 = 11400714785074694797
      }
      return buffer;
   }
}
