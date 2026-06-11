package org.infinispan.server.memcached;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.infinispan.testing.jupiter.tags.Fuzz;

import com.code_intelligence.jazzer.api.FuzzedDataProvider;
import com.code_intelligence.jazzer.junit.FuzzTest;

@Fuzz
public class ParseUtilFuzzTest {

   @FuzzTest(maxDuration = "5m")
   void fuzzReadLong(byte[] data) {
      try {
         ParseUtil.readLong(data);
      } catch (NumberFormatException e) {
         // expected for invalid input
      }
   }

   @FuzzTest(maxDuration = "5m")
   void fuzzRoundTrip(FuzzedDataProvider data) {
      long value = data.consumeLong();
      byte[] encoded = ParseUtil.writeAsciiLong(value);
      long decoded = ParseUtil.readLong(encoded);
      assertEquals(value, decoded);
   }
}
