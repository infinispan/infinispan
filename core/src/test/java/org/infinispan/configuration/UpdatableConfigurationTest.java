package org.infinispan.configuration;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.fail;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.logging.Log;
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
      configuration.update(null, builder.build());

      // Check that only the modified attributes have been updated
      assertEquals(-1, configuration.expiration().lifespan());
      assertEquals(3000, configuration.expiration().maxIdle());
   }

   public void testConfigurationComparison() {
      ConfigurationBuilder b1 = new ConfigurationBuilder();
      b1.encoding().mediaType(MediaType.TEXT_PLAIN.toString());
      Configuration c1 = b1.build();
      ConfigurationBuilder b2 = new ConfigurationBuilder();
      b2.encoding().mediaType(MediaType.APPLICATION_PROTOSTREAM_TYPE.toString());
      Configuration c2 = b2.build();
      try {
         c1.validateUpdate(null, c2);
         fail("Expected exception");
      } catch (Throwable t) {
         assertEquals(IllegalArgumentException.class, t.getClass());
         assertEquals(Log.CONFIG.invalidConfiguration("local-cache").getMessage(), t.getMessage());
         Throwable[] suppressed = t.getSuppressed();
         assertEquals(1, suppressed.length);
         assertEquals(Log.CONFIG.incompatibleAttribute("local-cache.encoding", "media-type", "text/plain", "application/x-protostream").getMessage(), suppressed[0].getMessage());
      }
   }
}
