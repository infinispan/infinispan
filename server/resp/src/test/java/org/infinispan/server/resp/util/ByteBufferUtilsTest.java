package org.infinispan.server.resp.util;

import static org.assertj.core.api.Assertions.assertThat;

import org.infinispan.server.resp.serialization.bytebuf.ByteBufferUtils;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "resp.util.ByteBufferUtilsTest")
public class ByteBufferUtilsTest {

   public void testLongStringSize() {
      long[] values = {
            Long.MIN_VALUE,
            -928491802948L,
            -123098L
            -1,
            0,
            1,
            2834784L,
            9284719827428L,
            101010101010101010L,
            Long.MAX_VALUE,
      };

      for (long value : values) {
         int expected = Long.toString(value).length();
         int actual = ByteBufferUtils.stringSize(value);
         assertThat(actual)
               .withFailMessage(() -> String.format("Failed handling: %d\nActual: %d Expected: %d", value, actual, expected))
               .isEqualTo(expected);
      }
   }
}
