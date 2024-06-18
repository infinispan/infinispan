package org.infinispan.commons.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.util.concurrent.TimeUnit;

import org.junit.Test;

public class TimeQuantityParserTest {
   @Test
   public void testParse() {
      assertIllegal("");
      assertIllegal("1.23.5");
      assertIllegal("1,23");
      assertIllegal("10.2");
      assertEquals(0, TimeQuantity.valueOf("0").longValue());
      assertEquals(1000, TimeQuantity.valueOf("1000ms").longValue());
      assertEquals(2000, TimeQuantity.valueOf("2000 ms").longValue());
      assertEquals(TimeUnit.HOURS.toMillis(1), TimeQuantity.valueOf("1h").longValue());
      assertEquals(TimeUnit.HOURS.toMillis(2), TimeQuantity.valueOf("2 h").longValue());
      assertEquals(TimeUnit.MINUTES.toMillis(1), TimeQuantity.valueOf("1m").longValue());
      assertEquals(TimeUnit.MINUTES.toMillis(2), TimeQuantity.valueOf("2 m").longValue());
      assertEquals(TimeUnit.SECONDS.toMillis(1), TimeQuantity.valueOf("1s").longValue());
      assertEquals(TimeUnit.SECONDS.toMillis(2), TimeQuantity.valueOf("2 s").longValue());
      assertEquals(TimeUnit.DAYS.toMillis(1), TimeQuantity.valueOf("1d").longValue());
      assertEquals(TimeUnit.DAYS.toMillis(2), TimeQuantity.valueOf("2 d").longValue());
      assertEquals(TimeUnit.MINUTES.toMillis(210), TimeQuantity.valueOf("3.5 h").longValue());
   }

   private void assertIllegal(String text) {
      assertThrows(IllegalArgumentException.class, () -> TimeQuantity.valueOf(text));
   }
}
