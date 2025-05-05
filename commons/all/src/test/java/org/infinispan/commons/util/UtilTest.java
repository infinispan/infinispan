package org.infinispan.commons.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.Test;

public class UtilTest {

   @Test
   public void testToStr() {
      assertThat(Util.toStr(null)).isEqualTo("[]");
      assertThat(Util.toStr("")).isEqualTo("");
      assertThat(Util.toStr(new byte[]{1, 2, 3})).isEqualTo("[B0x010203");
      assertThat(Util.toStr(new char[]{'1', '2', '3'})).isEqualTo("[1, 2, 3]");
      assertThat(Util.toStr(new boolean[]{true, false})).isEqualTo("[true, false]");
      assertThat(Util.toStr(new int[]{1, 2, 3})).isEqualTo("[1, 2, 3]");
      assertThat(Util.toStr(new long[]{1, 2, 3})).isEqualTo("[1, 2, 3]");
      assertThat(Util.toStr(new double[]{1, 2, 3})).isEqualTo("[1.0, 2.0, 3.0]");
      assertThat(Util.toStr(new float[]{1, 2, 3})).isEqualTo("[1.0, 2.0, 3.0]");
      assertThat(Util.toStr(new String[]{"a", "b", "c"})).isEqualTo("[a, b, c]");
      assertThat(Util.toStr(new Object[]{"a", "b", "c"})).isEqualTo("[a, b, c]");
      assertThat(Util.toStr(new Object[]{1, 2, 3})).isEqualTo("[1, 2, 3]");
      assertThat(Util.toStr(new Object[]{1L, 2L, 3L})).isEqualTo("[1, 2, 3]");
      assertThat(Util.toStr(new Object[]{1.0, 2.0, 3.0})).isEqualTo("[1.0, 2.0, 3.0]");
   }

   @Test
   public void testNewInstanceOrNull() {
      assertThat(Util.newInstanceOrNull(MyClass.class, new Class[] {String.class}, "a")).isInstanceOf(MyClass.class);
      assertThat(Util.newInstanceOrNull(MyClass.class, new Class[] {})).isNull();
   }

   @Test
   public void testComposeWithExceptions() {
      Runnable runnable = Util.composeWithExceptions(() -> {
         throw new RuntimeException("one");
      }, () -> {
         throw new RuntimeException("two");
      });
      assertThatThrownBy(runnable::run).isInstanceOf(RuntimeException.class).hasMessageContaining("one").hasSuppressedException(new RuntimeException("two"));
   }

   @Test
   public void testPrettyPrintTime() {
      assertThat(Util.prettyPrintTime(500)).isEqualTo("500 milliseconds");
      assertThat(Util.prettyPrintTime(1000)).isEqualTo("1 seconds");
      assertThat(Util.prettyPrintTime(400_000)).isEqualTo("6.67 minutes");
      assertThat(Util.prettyPrintTime(10_000_000)).isEqualTo("2.78 hours");
   }

   @Test
   public void testUnicodeEscapeUnescape() {
      assertThat(Util.unicodeEscapeString("a\\\t\n\r\fè")).isEqualTo("a\\\\\\t\\n\\r\\f\\u00E8");
      assertThat(Util.unicodeUnescapeString("\\u0061\\u0062\\u0063")).isEqualTo("abc");
   }

   static class MyClass {
      public MyClass(String s) {
      }
   }
}
