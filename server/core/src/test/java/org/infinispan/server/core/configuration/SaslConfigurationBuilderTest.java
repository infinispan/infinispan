package org.infinispan.server.core.configuration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;

import org.testng.annotations.Test;

/**
 * Tests for {@link SaslConfigurationBuilder}.
 */
@Test(groups = "unit", testName = "server.core.SaslConfigurationBuilderTest")
public class SaslConfigurationBuilderTest {

   public void testMechPropertiesIncludeXmlSuppliedProperties() {
      SaslConfigurationBuilder builder = new SaslConfigurationBuilder();
      // The parser adds the DIGEST realm list before parsing <sasl> child elements; XML-supplied properties must survive that; see https://github.com/infinispan/infinispan/issues/18371
      builder.addMechProperty("wildfly.security.sasl.digest.realm", "default");
      builder.addProperty("wildfly.sasl.scram.max-iteration-count", "262144");

      Map<String, String> props = builder.create().mechProperties();
      assertEquals(2, props.size());
      assertTrue(props.containsKey("wildfly.security.sasl.digest.realm"));
      assertEquals("default", props.get("wildfly.security.sasl.digest.realm"));
      assertTrue(props.containsKey("wildfly.sasl.scram.max-iteration-count"));
      assertEquals("262144", props.get("wildfly.sasl.scram.max-iteration-count"));
   }

   public void testMechPropertiesWithoutProgrammaticAdditions() {
      SaslConfigurationBuilder builder = new SaslConfigurationBuilder();
      builder.addProperty("wildfly.sasl.scram.min-iteration-count", "8192");

      Map<String, String> props = builder.create().mechProperties();
      assertEquals(1, props.size());
      assertEquals("8192", props.get("wildfly.sasl.scram.min-iteration-count"));
   }
}
