package org.infinispan.server.core.configuration;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Map;

import org.testng.annotations.Test;

/**
 * Test for {@link SaslConfigurationBuilder}.
 */
@Test(groups = "unit", testName = "server.configuration.SaslConfigurationTest")
public class SaslConfigurationTest {

   public void testMechPropertiesNotDroppedWhenExplicitAuthPresent() {
      SaslConfigurationBuilder sasl = new SaslConfigurationBuilder();
      sasl.addMechProperty("wildfly.sasl.realm-list", "default");
      sasl.addProperty("wildfly.sasl.scram.max-iteration-count", "262144");

      Map<String, String> props = sasl.create().mechProperties();
      assertEquals(2, props.size(), "XML properties should not be dropped when explicit mech properties exist");
      assertEquals("default", props.get("wildfly.sasl.realm-list"));
      assertEquals("262144", props.get("wildfly.sasl.scram.max-iteration-count"));
   }
}
