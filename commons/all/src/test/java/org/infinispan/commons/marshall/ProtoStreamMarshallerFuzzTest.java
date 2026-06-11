package org.infinispan.commons.marshall;

import java.io.IOException;

import org.infinispan.testing.jupiter.tags.Fuzz;

import com.code_intelligence.jazzer.api.FuzzedDataProvider;
import com.code_intelligence.jazzer.junit.FuzzTest;

@Fuzz
public class ProtoStreamMarshallerFuzzTest {

   private static final ProtoStreamMarshaller marshaller = new ProtoStreamMarshaller();

   @FuzzTest
   void fuzzObjectFromByteBuffer(byte[] data) {
      try {
         marshaller.objectFromByteBuffer(data, 0, data.length);
      } catch (IOException | ClassNotFoundException | IllegalStateException e) {
         //  expected for malformed input
      }
   }

   @FuzzTest
   void fuzzRoundTrip(FuzzedDataProvider data) throws Exception {
      String value = data.consumeRemainingAsString();
      if (value.isEmpty()) return;

      byte[] encoded = marshaller.objectToByteBuffer(value);
      Object decoded = marshaller.objectFromByteBuffer(encoded);
      if (!value.equals(decoded)) {
         throw new AssertionError("Roundtrip failed: input=" + value + " output=" + decoded);
      }
   }
}
