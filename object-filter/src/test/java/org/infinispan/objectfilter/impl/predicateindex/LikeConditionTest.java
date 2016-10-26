package org.infinispan.objectfilter.impl.predicateindex;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

/**
 * @author anistor@redhat.com
 * @since 9.0
 */
public class LikeConditionTest {

   @Test
   public void testDegeneratedEquals() throws Exception {
      LikeCondition likeCondition = new LikeCondition("ab");
      assertTrue(likeCondition.match("ab"));
      assertFalse(likeCondition.match("ac"));
      assertFalse(likeCondition.match("XabY"));
   }

   @Test
   public void testDegeneratedContains() throws Exception {
      LikeCondition likeCondition = new LikeCondition("%ab%");
      assertTrue(likeCondition.match("ab"));
      assertTrue(likeCondition.match("xabxx"));
      assertFalse(likeCondition.match("axb"));
   }

   @Test
   public void testDegeneratedStartsWith() throws Exception {
      assertTrue(new LikeCondition("ab%").match("ab"));
      assertTrue(new LikeCondition("ab%").match("abx"));
      assertTrue(new LikeCondition("ab%").match("abxx"));
      assertFalse(new LikeCondition("ab%").match("xab"));
      assertFalse(new LikeCondition("ab%").match("axb"));

      assertTrue(new LikeCondition("ab_").match("abc"));
      assertFalse(new LikeCondition("ab_").match("ab"));
      assertFalse(new LikeCondition("ab_").match("abxx"));
      assertFalse(new LikeCondition("ab_").match("xab"));
      assertFalse(new LikeCondition("ab_").match("xaby"));
   }

   @Test
   public void testDegeneratedEndsWith() throws Exception {
      assertTrue(new LikeCondition("%ab").match("ab"));
      assertTrue(new LikeCondition("%ab").match("xab"));
      assertTrue(new LikeCondition("%ab").match("xxab"));
      assertFalse(new LikeCondition("%ab").match("abx"));
      assertFalse(new LikeCondition("%ab").match("axb"));

      assertTrue(new LikeCondition("_ab").match("cab"));
      assertFalse(new LikeCondition("_ab").match("ab"));
      assertFalse(new LikeCondition("_ab").match("xxab"));
      assertFalse(new LikeCondition("_ab").match("abc"));
      assertFalse(new LikeCondition("_ab").match("xabc"));
   }

   @Test
   public void testSingleCharWildcard() throws Exception {
      LikeCondition likeCondition = new LikeCondition("a_b_c");
      assertTrue(likeCondition.match("aXbYc"));
      assertTrue(likeCondition.match("a_b_c"));
      assertTrue(likeCondition.match("a%b%c"));
      assertFalse(likeCondition.match("abc"));
      assertFalse(likeCondition.match("aXXbYYc"));
   }

   @Test
   public void testMultipleCharWildcard() throws Exception {
      LikeCondition likeCondition = new LikeCondition("a%b%c");
      assertTrue(likeCondition.match("abc"));
      assertTrue(likeCondition.match("aXbc"));
      assertTrue(likeCondition.match("aXYbZc"));
      assertTrue(likeCondition.match("a_b_c"));
      assertTrue(likeCondition.match("a%b%c"));
      assertFalse(likeCondition.match("a"));
      assertFalse(likeCondition.match("aX"));
      assertFalse(likeCondition.match("a%"));
   }

   @Test
   public void testEscapeChar() {
      assertTrue(new LikeCondition("a\\%b").match("a%b"));
      assertFalse(new LikeCondition("a\\%b").match("aXb"));
      assertFalse(new LikeCondition("a\\%b").match("ab"));
      assertTrue(new LikeCondition("a\\\\b").match("a\\b"));
      assertTrue(new LikeCondition("a~%b", '~').match("a%b"));
      assertFalse(new LikeCondition("a~%b", '~').match("aXb"));
      assertFalse(new LikeCondition("a~%b", '~').match("ab"));
      assertTrue(new LikeCondition("a~~b", '~').match("a~b"));
   }

   @Test
   public void testPlusEscaping() throws Exception {
      LikeCondition likeCondition = new LikeCondition("a%aZ+");
      assertTrue(likeCondition.match("aaaZ+"));
      assertFalse(likeCondition.match("aaa"));
      assertFalse(likeCondition.match("aaaZ"));
      assertFalse(likeCondition.match("aaaZZ"));
      assertFalse(likeCondition.match("aaaZZZ"));
   }

   @Test
   public void testAsteriskEscaping() throws Exception {
      LikeCondition likeCondition = new LikeCondition("a%aZ*");
      assertTrue(likeCondition.match("aaaZ*"));
      assertFalse(likeCondition.match("aaa"));
      assertFalse(likeCondition.match("aaaZ"));
      assertFalse(likeCondition.match("aaaZZ"));
      assertFalse(likeCondition.match("aaaZZZ"));
   }

   @Test
   public void testGeneralMetacharEscaping() {
      assertTrue(new LikeCondition("a%(b").match("aaa(b"));
      assertTrue(new LikeCondition("a%)b").match("aaa)b"));
      assertTrue(new LikeCondition("a%[b").match("aaa[b"));
      assertTrue(new LikeCondition("a%]b").match("aaa]b"));
      assertTrue(new LikeCondition("a%{b").match("aaa{b"));
      assertTrue(new LikeCondition("a%}b").match("aaa}b"));
      assertTrue(new LikeCondition("a%$b").match("aaa$b"));
      assertTrue(new LikeCondition("a%^b").match("aaa^b"));
      assertTrue(new LikeCondition("a%.b").match("aaa.b"));
      assertTrue(new LikeCondition("a%|b").match("aaa|b"));
      assertTrue(new LikeCondition("a%\\b").match("aaa\\b"));
   }
}
