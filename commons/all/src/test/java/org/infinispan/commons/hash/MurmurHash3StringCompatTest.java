package org.infinispan.commons.hash;

import java.util.Random;

import org.junit.Assert;
import org.junit.Test;

/**
 * @author Dan Berindei
 * @since 9.0
 */
public class MurmurHash3StringCompatTest {
   private static final long NUM_KEYS = 100000;
   private static final int MAX_KEY_SIZE = 5;

   @Test
   public void compareHashes() {
      Random random = new Random(9005);
      for (int i = 0; i < NUM_KEYS; i++) {
         int cpLen = Math.abs(random.nextInt(MAX_KEY_SIZE) - random.nextInt(MAX_KEY_SIZE)) + 1;
         int[] codePoints = random.ints(cpLen, 0, 0x110000).filter(Character::isDefined).toArray();
         String s = new String(codePoints, 0, codePoints.length);
         testString(s);
      }
   }

   private void testString(String s) {
      int h1 = MurmurHash3Old.getInstance().hash(s);
      int h2 = MurmurHash3.getInstance().hash(s);
      Assert.assertEquals(h1, h2);
   }
}
