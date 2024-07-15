package org.infinispan.commons.util;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.Test;

public class GlobMatcherTest {

   @Test
   public void testFuzzMatcher() {
      byte[] str = new byte[32];
      byte[] pat = new byte[32];

      int cycles = 10_000_000;
      while (cycles-- > 0) {
         ThreadLocalRandom.current().nextBytes(str);
         nextBytes(pat);
         GlobMatcher.match(str, pat);
      }
   }

   private void nextBytes(byte[] bytes) {
      for (int i = 0, len = bytes.length; i < len; )
         for (int rnd = ThreadLocalRandom.current().nextInt(0x80),
              n = Math.min(len - i, Integer.SIZE/Byte.SIZE);
              n-- > 0;)
            bytes[i++] = (byte) rnd;
   }

   @Test
   public void testMultiByteString() {
      assertThrows(AssertionError.class, () -> GlobMatcher.match("p?tte*n", "José"));
      assertThrows(AssertionError.class, () -> GlobMatcher.match("p?tte*n".getBytes(StandardCharsets.US_ASCII), "José".getBytes()));
      assertThrows(AssertionError.class, () -> GlobMatcher.match("p?tte*n", "€10,00"));
      assertThrows(AssertionError.class, () -> GlobMatcher.match("p?tte*n".getBytes(StandardCharsets.US_ASCII), "€10,00".getBytes()));

      assertTrue(GlobMatcher.match("Jo?*", "Jose"));
      assertTrue(GlobMatcher.match("$[1-9]*,[0-9][0-9]", "$101,23"));
      assertTrue(GlobMatcher.match("$[1-9]*,[0-9][0-9]".getBytes(StandardCharsets.US_ASCII), "$101,23".getBytes(StandardCharsets.US_ASCII)));
   }

   @Test
   public void testValidInputs() {
      assertFalse(GlobMatcher.match("a*b", "aaa"));
      assertFalse(GlobMatcher.match("a*a*b", "aaaa"));

      // Hangs when parsing as regex.
      assertFalse(GlobMatcher.match("a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*b", "a".repeat(55)));

      assertTrue(GlobMatcher.match("a*a*b", "aaa7b"));
      assertTrue(GlobMatcher.match("a*[0-9]b", "aH5b"));
      assertTrue(GlobMatcher.match("a*[k\\-0-9]b", "aHk5b"));
      assertTrue(GlobMatcher.match("a*[k\\-0-9]b", "aH-5b"));
      assertFalse(GlobMatcher.match("a*[k\\-0-9]b", "aHb"));
      assertFalse(GlobMatcher.match("a*[0-9]b", "aHRb"));
      assertTrue(GlobMatcher.match("a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*b", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab"));
      assertTrue(GlobMatcher.match("hello_[24]", "hello_4"));
      assertTrue(GlobMatcher.match("hello_[24]", "hello_2"));
   }

   @Test
   public void testValidInputBytes() {
      assertFalse(GlobMatcher.match(new byte[] { 'a', '*', 'b'}, new byte[] { 'a', 'a', 'a' }));
      assertTrue(GlobMatcher.match(new byte[] { 'a', '*', 'b'}, new byte[] { 'a', 'a', 'a', 'b' }));
      assertTrue(GlobMatcher.match(new byte[] { 'a', '*', '[', '0', '-', '9', ']', 'b'}, new byte[] { 'a', 'H', '5', 'b' }));
   }
}
