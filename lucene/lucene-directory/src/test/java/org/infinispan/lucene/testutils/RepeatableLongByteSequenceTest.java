package org.infinispan.lucene.testutils;

import org.testng.annotations.Test;

/**
 * Test for {@link RepeatableLongByteSequence}
 *
 * @author Dan Berindei
 * @since 5.2
 */
@Test(groups = "unit", testName = "lucene.testutils.RepeatableLongByteSequenceTest")
public class RepeatableLongByteSequenceTest {
   @Test(description="To verify the RepeatableLongByteSequence meets the requirement of producing "
      + "always the same values when using the single nextByte()")
   public void verifyRepeatability() {
      RepeatableLongByteSequence src1 = new RepeatableLongByteSequence();
      RepeatableLongByteSequence src2 = new RepeatableLongByteSequence();
      for (int i = 0; i < 1000; i++) {
         assert src1.nextByte() == src2.nextByte();
      }
   }

   @Test(description="To verify the RepeatableLongByteSequence meets the requirement of producing "
         + "always the same values when using the multivalued nextBytes()")
   public void verifyEquality() {
      RepeatableLongByteSequence src1 = new RepeatableLongByteSequence();
      RepeatableLongByteSequence src2 = new RepeatableLongByteSequence();
      final int arrayLength = 10;
      byte[] b = new byte[arrayLength];
      for (int i = 0; i < 1000; i++) {
         if((i % arrayLength) == 0) {
            src1.nextBytes(b);
         }
         assert b[i % arrayLength] == src2.nextByte();
      }
   }
}
