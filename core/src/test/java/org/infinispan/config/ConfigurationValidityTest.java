package org.infinispan.config;

import org.testng.annotations.Test;

@Test(groups = "unit", testName = "config.ConfigurationValidityTest")
public class ConfigurationValidityTest {
   public void testInvalidConfigs() {
      Configuration c = new Configuration();
      c.setCacheMode(Configuration.CacheMode.DIST_SYNC);
      c.assertValid();
      c.setCacheMode(Configuration.CacheMode.DIST_ASYNC);
      try {
         c.assertValid();
         assert false : "Should fail";
      } catch (ConfigurationException expected) {
      }
      c.setUnsafeUnreliableReturnValues(true);
      c.assertValid();
      c.setCacheMode(Configuration.CacheMode.DIST_SYNC);
      c.assertValid();
      c.setFetchInMemoryState(true);
      try {
         c.assertValid();
         assert false : "Should fail";
      } catch (ConfigurationException expected) {
      }
   }
}
