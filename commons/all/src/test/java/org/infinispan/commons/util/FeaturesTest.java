package org.infinispan.commons.util;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

public class FeaturesTest {

   @Test
   public void testFeatures() {
      Features features = new Features();
      assertFalse(features.isAvailable("A"));
      assertTrue(features.isAvailable("B"));
   }

   @Test
   public void featureSysOverride() {
      Features features = new Features();
      assertFalse(features.isAvailable("A"));
      System.setProperty("org.infinispan.feature.A", "true");
      System.setProperty("org.infinispan.feature.B", "false");
      boolean a = features.isAvailable("A");
      boolean b = features.isAvailable("B");
      System.clearProperty("org.infinispan.feature.A");
      System.clearProperty("org.infinispan.feature.B");
      assertTrue(a);
      assertFalse(b);
   }
}
