package org.infinispan.commons.hash;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

public class MurmurHash3ObjectHashTest {

   private static final MurmurHash3 HASH = MurmurHash3.getInstance();
   private static final int SEED = 0;

   @Test
   public void testEmptyLongArrayHashesLikeLongArrayOverload() {
      // Hash through the Object overload, the one the hot key tracker uses.
      Object empty = new long[0];
      Object nonEmpty = new long[]{1L};

      assertThat(HASH.MurmurHash3_x64_64(empty, SEED))
            .as("an empty long[] seen as Object must hash like the long[] overload")
            .isEqualTo(MurmurHash3.MurmurHash3_x64_64(new long[0], SEED));
      assertThat(HASH.MurmurHash3_x64_64(nonEmpty, SEED))
            .as("a non-empty long[] must not hash like an empty one")
            .isNotEqualTo(HASH.MurmurHash3_x64_64(empty, SEED));
   }
}
