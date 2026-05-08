package org.infinispan.server.test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;

import org.infinispan.server.test.core.compatibility.CompatibilityEntry;
import org.junit.jupiter.api.Test;

public class CompatibilityEntryTest {

   @Test
   public void testMatchesVersions() {
      CompatibilityEntry entry = new CompatibilityEntry("[16.0,16.1)", "[16.1,)", Map.of(), List.of());
      assertTrue(entry.matchesVersions("16.0.4", "16.1.1"));
      assertTrue(entry.matchesVersions("16.0.4", "16.2.0"));
      assertFalse(entry.matchesVersions("16.0.4", "16.0.5"));
   }
}
