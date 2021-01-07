package org.infinispan.configuration;

import static org.testng.AssertJUnit.assertEquals;

import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.testng.annotations.Test;

@Test(groups = "functional", testName= "configuration.UpdatableConfigurationTest")
public class UpdatableConfigurationTest {

   public void testUpdateConfiguration() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.expiration().maxIdle(1000).lifespan(2000);
      Configuration configuration = builder.build();

      // Check that attributes are correct before the update
      assertEquals(1000, configuration.expiration().maxIdle());
      assertEquals(2000, configuration.expiration().lifespan());

      builder = new ConfigurationBuilder();
      builder.expiration().maxIdle(3000); // Lifespan will be set to the default
      configuration.update(builder.build());

      // Check that only the modified attributes have been updated
      assertEquals(-1, configuration.expiration().lifespan());
      assertEquals(3000, configuration.expiration().maxIdle());
   }
}
