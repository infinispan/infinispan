package org.infinispan.functional.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.util.List;

import org.testng.annotations.Test;

@Test(groups = "unit", testName = "functional.impl.StatsEnvelopeTest")
public class StatsEnvelopeTest {

   public void testUnpackWrappedValue() {
      StatsEnvelope<String> envelope = StatsEnvelope.create("value", false);
      assertEquals("value", StatsEnvelope.unpack(null, null, envelope));
   }

   public void testUnpackAlreadyUnwrappedValue() {
      Object raw = "raw-unwrapped-value";
      assertSame(raw, StatsEnvelope.unpack(null, null, raw));
   }

   public void testUnpackCollectionOfWrappedValues() {
      StatsEnvelope<String> a = StatsEnvelope.create("a", false);
      StatsEnvelope<String> b = StatsEnvelope.create("b", false);
      Object result = StatsEnvelope.unpackCollection(null, null, List.of(a, b));
      assertEquals(List.of("a", "b"), result);
   }

   public void testUnpackCollectionOfAlreadyUnwrappedValues() {
      List<String> raw = List.of("a", "b");
      assertSame(raw, StatsEnvelope.unpackCollection(null, null, raw));
   }

   public void testUnpackCollectionOfEmptyCollection() {
      Object result = StatsEnvelope.unpackCollection(null, null, List.of());
      assertEquals(List.of(), result);
   }
}
