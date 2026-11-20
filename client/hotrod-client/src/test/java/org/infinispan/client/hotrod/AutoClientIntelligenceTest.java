package org.infinispan.client.hotrod;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.infinispan.client.hotrod.configuration.ClientIntelligence;
import org.testng.annotations.Test;

/**
 * Tests for AUTO client intelligence mode which starts as HASH_DISTRIBUTION_AWARE
 * and degrades to BASIC when topology connections fail.
 *
 * @since 16.4
 */
@Test(groups = "unit", testName = "client.hotrod.AutoClientIntelligenceTest")
public class AutoClientIntelligenceTest {

   public void testEffectiveIntelligenceMethod() {
      // Unit test for the getEffectiveIntelligence() method
      assertEquals(ClientIntelligence.HASH_DISTRIBUTION_AWARE, ClientIntelligence.AUTO.getEffectiveIntelligence(),
            "AUTO should have effective intelligence of HASH_DISTRIBUTION_AWARE");
      assertEquals(ClientIntelligence.BASIC, ClientIntelligence.BASIC.getEffectiveIntelligence(),
            "BASIC effective intelligence should be itself");
      assertEquals(ClientIntelligence.TOPOLOGY_AWARE, ClientIntelligence.TOPOLOGY_AWARE.getEffectiveIntelligence(),
            "TOPOLOGY_AWARE effective intelligence should be itself");
      assertEquals(ClientIntelligence.HASH_DISTRIBUTION_AWARE, ClientIntelligence.HASH_DISTRIBUTION_AWARE.getEffectiveIntelligence(),
            "HASH_DISTRIBUTION_AWARE effective intelligence should be itself");
   }

   public void testIsTopologyAwareMethod() {
      // Unit test for the isTopologyAware() method
      assertTrue(ClientIntelligence.AUTO.isTopologyAware(),
            "AUTO should be topology-aware");
      assertTrue(ClientIntelligence.TOPOLOGY_AWARE.isTopologyAware(),
            "TOPOLOGY_AWARE should be topology-aware");
      assertTrue(ClientIntelligence.HASH_DISTRIBUTION_AWARE.isTopologyAware(),
            "HASH_DISTRIBUTION_AWARE should be topology-aware");
      assertFalse(ClientIntelligence.BASIC.isTopologyAware(),
            "BASIC should not be topology-aware");
   }

   public void testAutoValueAssignment() {
      // Verify AUTO has the correct value
      assertEquals(4, ClientIntelligence.AUTO.getValue(),
            "AUTO should have value 4");
      assertEquals(1, ClientIntelligence.BASIC.getValue(),
            "BASIC should have value 1");
      assertEquals(2, ClientIntelligence.TOPOLOGY_AWARE.getValue(),
            "TOPOLOGY_AWARE should have value 2");
      assertEquals(3, ClientIntelligence.HASH_DISTRIBUTION_AWARE.getValue(),
            "HASH_DISTRIBUTION_AWARE should have value 3");
   }

   public void testAutoEnumOrdering() {
      // Verify all intelligence modes exist
      ClientIntelligence[] values = ClientIntelligence.values();
      assertEquals(4, values.length, "Should have exactly 4 intelligence modes");

      // Verify they can all be retrieved
      assertEquals(ClientIntelligence.BASIC, ClientIntelligence.valueOf("BASIC"));
      assertEquals(ClientIntelligence.TOPOLOGY_AWARE, ClientIntelligence.valueOf("TOPOLOGY_AWARE"));
      assertEquals(ClientIntelligence.HASH_DISTRIBUTION_AWARE, ClientIntelligence.valueOf("HASH_DISTRIBUTION_AWARE"));
      assertEquals(ClientIntelligence.AUTO, ClientIntelligence.valueOf("AUTO"));
   }

   public void testDefaultIntelligenceUnchanged() {
      // Verify that the default intelligence is still HASH_DISTRIBUTION_AWARE
      assertEquals(ClientIntelligence.HASH_DISTRIBUTION_AWARE, ClientIntelligence.getDefault(),
            "Default intelligence should remain HASH_DISTRIBUTION_AWARE for backward compatibility");
   }
}
