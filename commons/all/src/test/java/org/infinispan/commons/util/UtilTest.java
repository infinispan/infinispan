package org.infinispan.commons.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.hash.CRC16;
import org.infinispan.commons.hash.MurmurHash3;
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
      assertThat(Util.unicodeEscapeString("ab\t\\\t\n\r\fè")).isEqualTo("ab\\t\\\\\\t\\n\\r\\f\\u00E8");
      assertThat(Util.unicodeUnescapeString("\\u0061\\u0062\\u0063\\u00De")).isEqualTo("abcÞ");
      assertThat(Util.unicodeUnescapeString("u\\\t\\\r\\\n\\\fo")).isEqualTo("u\t\r\n\fo");

      assertThatThrownBy(() -> Util.unicodeUnescapeString("\\u1BdG"))
            .isInstanceOf(IllegalArgumentException.class);
   }

   @Test
   public void testArrayEquals() {
      byte[] a = {'b', 'a', 'l', 'l', 'o', 'o', 'n'};
      byte[] b = {'m', 'a', 'l', 'l', 'o', 'c', '.'};

      assertThat(Util.arraysEqual(a, 1, 3, b, 1, 3)).isTrue();
      assertThat(Util.arraysEqual(a, 1, 3, b, 2, 3)).isFalse();
      assertThat(Util.arraysEqual(a, 0, 2, b, 0, 2)).isFalse();
      assertThat(Util.arraysEqual(a, 1, 6, b, 1, 6)).isFalse();
   }

   @Test
   public void testConcat() {
      byte[] a = {'h', 'a'};
      byte[] b = {'n', 'd'};
      byte[] hand = {'h', 'a', 'n', 'd'};

      assertThat(Util.concat(a, b)).isEqualTo(hand);

      String[] hey = new String[] {"h", "e", "y"};
      assertThat(Util.concat(new String[] {"h", "e"}, "y")).isEqualTo(hey);
   }

   @Test
   public void testStr() {
      assertThat(Util.toStr((Collection<?>) null)).isEqualTo("[]");
      assertThat(Util.toStr(Collections.emptyList())).isEqualTo("[]");
      assertThat(Util.toStr(List.of(1, 2))).isEqualTo("[1, 2]");
      assertThat(Util.toStr(IntStream.range(0, 20).boxed().toList())).isEqualTo("[0, 1, 2, 3, 4, 5, 6, 7...<12 other elements>]");

      List<Object> a = new ArrayList<>();
      a.add(1);
      a.add(2);
      a.add(a);
      assertThat(Util.toStr(a)).isEqualTo("[1, 2, (this Collection)]");
   }

   @Test
   public void testHexDump() {
      byte[] smallDatum = new byte[32];
      ThreadLocalRandom.current().nextBytes(smallDatum);
      String h = Util.hexDump(smallDatum);
      assertThat(h).isNotEmpty();
      assertThat(Util.hexDump(ByteBuffer.wrap(smallDatum))).isEqualTo(h);

      byte[] largeDatum = new byte[128];
      ThreadLocalRandom.current().nextBytes(largeDatum);

      h = Util.hexDump(largeDatum);
      assertThat(h).endsWith(" bytes)");
      assertThat(Util.hexDump(ByteBuffer.wrap(largeDatum))).isEqualTo(h);
   }

   @Test
   public void testUnwrapExceptionMessage() {
      assertThat(Util.unwrapExceptionMessage(new IllegalStateException("message 1"))).isEqualTo("message 1");

      IllegalStateException e1 = new IllegalStateException("message 1");
      IllegalStateException e2 = new IllegalStateException("message 2");
      e1.addSuppressed(e2);
      assertThat(Util.unwrapExceptionMessage(e1))
            .isEqualTo("message 1\n    message 2");

      e1 = new IllegalStateException("message");
      e2 = new IllegalStateException("message");
      e1.addSuppressed(e2);
      assertThat(Util.unwrapExceptionMessage(e1)).isEqualTo("message");
   }

   @Test
   public void testGetInstance() {
      class A {
         private A() { }
      }

      assertThat(Util.<String>getInstance(String.class.getName(), Util.class.getClassLoader())).isNotNull();
      assertThat(Util.<CRC16>getInstance(CRC16.class.getName(), Util.class.getClassLoader())).isEqualTo(CRC16.getInstance());
      assertThatThrownBy(() -> Util.<A>getInstance(A.class.getName(), Util.class.getClassLoader()))
            .isInstanceOf(CacheConfigurationException.class);
   }

   @Test
   public void testPrintArray() {
      byte[] arr = {'h', 'i'};
      assertThat(Util.printArray(arr, true)).isEqualTo("[B0x6869,h=10c2]");

      arr = new byte[32];
      ThreadLocalRandom.current().nextBytes(arr);
      assertThat(Util.printArray(arr, true))
            .contains("..[32],h=")
            .startsWith("[B0x");
   }

   @Test
   public void testSegmentSize() {
      assertThat(Util.getSegmentSize(CRC16.getInstance(), 256)).isEqualTo(256);
      assertThat(Util.getSegmentSize(MurmurHash3.getInstance(), 256)).isEqualTo(8388608);
   }

   static class MyClass {
      public MyClass(String s) {
      }
   }
}
