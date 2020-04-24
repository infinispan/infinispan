package org.infinispan.commons.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import org.junit.Test;

public class ByteQuantityParserTest {
   @Test
   public void testParse() {
      assertIllegal("");
      assertIllegal("-1");
      assertIllegal("1.23.5");
      assertIllegal("1,23");
      assertIllegal("10.2");
      assertIllegal("10.2 kB");
      assertIllegal("0.0000001 MB");
      assertIllegal("100000000000000000000 GB");
      assertEquals(0, ByteQuantity.parse("0"));
      assertEquals(250_000, ByteQuantity.parse("250000 B"));
      assertEquals(1_000, ByteQuantity.parse("1000"));
      assertEquals(1_000, ByteQuantity.parse("1KB"));
      assertEquals(1_024, ByteQuantity.parse("1KiB"));
      assertEquals(1_000, ByteQuantity.parse("1 KB"));
      assertEquals(1_000, ByteQuantity.parse("1  KB"));
      assertEquals(1_000_000, ByteQuantity.parse("1 MB"));
      assertEquals(1_048_576, ByteQuantity.parse("1 MiB"));
      assertEquals(1_523_000, ByteQuantity.parse("1.523 MB"));
      assertEquals(100_000, ByteQuantity.parse("0.1 MB"));
      assertEquals(1, ByteQuantity.parse("0.000001 MB"));
      assertEquals(10_000_000_000L, ByteQuantity.parse("10GB"));
      assertEquals(1_000_000_000_000L, ByteQuantity.parse("1TB"));
      assertEquals(1_099_511_627_776L, ByteQuantity.parse("1TiB"));
      assertEquals(100_000_000_000_000L, ByteQuantity.parse("100TB"));
   }

   private void assertIllegal(String text) {
      assertThrows(IllegalArgumentException.class, () -> ByteQuantity.parse(text));
   }
}
