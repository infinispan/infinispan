package org.infinispan.server.resp.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;

import org.assertj.core.api.ThrowableAssert;
import org.infinispan.server.resp.commands.ArgumentUtils;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "resp.util.ArgumentUtilsTest")
public class ArgumentUtilsTest {

   public void testReadingDoubleValues() {
      double[] values = {
            Double.NEGATIVE_INFINITY,
            Double.MIN_VALUE,
            -129873.2387,
            -0.3847823,
            0,
            0.398475783,
            2349879,
            298378234.3487832,
            Double.MAX_VALUE,
            Double.POSITIVE_INFINITY,
      };

      for (double value : values) {
         byte[] bytes = Double.toString(value).getBytes(StandardCharsets.US_ASCII);
         assertThat(ArgumentUtils.toDouble(bytes)).isEqualTo(value);
      }

      // Test the infinity arguments from RESP.
      assertThat(ArgumentUtils.toDouble(new byte[] { 'i', 'n', 'f'})).isEqualTo(Double.POSITIVE_INFINITY);
      assertThat(ArgumentUtils.toDouble(new byte[] { '-', 'i', 'n', 'f'})).isEqualTo(Double.NEGATIVE_INFINITY);

      // Test invalid values.
      assertInvalidNumber(() -> ArgumentUtils.toDouble(" 1.0".getBytes(StandardCharsets.US_ASCII)));
      assertInvalidNumber(() -> ArgumentUtils.toDouble("string".getBytes(StandardCharsets.US_ASCII)));
   }

   public void testReadingLongValues() {
      long[] values = {
            Long.MIN_VALUE,
            -298471982737L,
            -1,
            0,
            1,
            9284774833948L,
            Long.MAX_VALUE,
      };

      for (long value : values) {
         byte[] bytes = Long.toString(value).getBytes(StandardCharsets.US_ASCII);
         assertThat(ArgumentUtils.toLong(bytes)).isEqualTo(value);
      }

      // Invalid values.
      assertInvalidNumber(() -> ArgumentUtils.toLong("string".getBytes(StandardCharsets.US_ASCII)));
      assertInvalidNumber(() -> ArgumentUtils.toLong(Double.toString(1.2398).getBytes(StandardCharsets.US_ASCII)));
   }

   private void assertInvalidNumber(ThrowableAssert.ThrowingCallable callable) {
      assertThatThrownBy(callable).isInstanceOf(NumberFormatException.class);
   }
}
