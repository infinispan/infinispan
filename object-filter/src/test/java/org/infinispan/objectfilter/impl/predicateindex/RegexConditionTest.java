package org.infinispan.objectfilter.impl.predicateindex;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author anistor@redhat.com
 * @since 9.0
 */
public class RegexConditionTest {

   @Test
   public void testSingleWildcard() throws Exception {
      RegexCondition regexCondition = new RegexCondition("a_b");
      assertTrue(regexCondition.match("aXb"));
      assertTrue(regexCondition.match("a_b"));
      assertTrue(regexCondition.match("a%b"));

      assertFalse(regexCondition.match("ab"));
      assertFalse(regexCondition.match("aXXb"));
   }

   @Test
   public void testMultipleWildcard() throws Exception {
      RegexCondition regexCondition = new RegexCondition("a%b");
      assertTrue(regexCondition.match("ab"));
      assertTrue(regexCondition.match("aXb"));
      assertTrue(regexCondition.match("aXYb"));
      assertTrue(regexCondition.match("a_b"));
      assertTrue(regexCondition.match("a%b"));

      assertFalse(regexCondition.match("a"));
      assertFalse(regexCondition.match("aX"));
      assertFalse(regexCondition.match("a%"));
   }

   @Test
   public void testPlusEscaping() throws Exception {
      RegexCondition regexCondition = new RegexCondition("a%aZ+");
      assertTrue(regexCondition.match("aaaZ+"));

      assertFalse(regexCondition.match("aaa"));
      assertFalse(regexCondition.match("aaaZ"));
      assertFalse(regexCondition.match("aaaZZ"));
      assertFalse(regexCondition.match("aaaZZZ"));
   }

   @Test
   public void testAsteriskEscaping() throws Exception {
      RegexCondition regexCondition = new RegexCondition("a%aZ*");
      assertTrue(regexCondition.match("aaaZ*"));

      assertFalse(regexCondition.match("aaa"));
      assertFalse(regexCondition.match("aaaZ"));
      assertFalse(regexCondition.match("aaaZZ"));
      assertFalse(regexCondition.match("aaaZZZ"));
   }

   @Test
   public void testGeneralMetacharEscaping() {
      assertTrue(new RegexCondition("a%(b").match("aaa(b"));
      assertTrue(new RegexCondition("a%)b").match("aaa)b"));
      assertTrue(new RegexCondition("a%[b").match("aaa[b"));
      assertTrue(new RegexCondition("a%]b").match("aaa]b"));
      assertTrue(new RegexCondition("a%{b").match("aaa{b"));
      assertTrue(new RegexCondition("a%}b").match("aaa}b"));
      assertTrue(new RegexCondition("a%$b").match("aaa$b"));
      assertTrue(new RegexCondition("a%^b").match("aaa^b"));
      assertTrue(new RegexCondition("a%.b").match("aaa.b"));
      assertTrue(new RegexCondition("a%|b").match("aaa|b"));
      assertTrue(new RegexCondition("a%\\b").match("aaa\\b"));
   }
}
