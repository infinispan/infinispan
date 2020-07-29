package org.infinispan.xsite;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "xsite.XSiteExpirationTest")
public class XSiteExpirationTest extends AbstractTwoSitesTest {
   @Override
   protected ConfigurationBuilder getNycActiveConfig() {
      return null;
   }

   @Override
   protected ConfigurationBuilder getLonActiveConfig() {
      return null;
   }
}
