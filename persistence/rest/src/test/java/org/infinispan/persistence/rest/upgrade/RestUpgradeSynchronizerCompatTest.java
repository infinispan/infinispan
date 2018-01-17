package org.infinispan.persistence.rest.upgrade;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.testng.annotations.Test;

@Test(testName = "persistence.rest.upgrade.RestUpgradeSynchronizerCompatTest", groups = "functional")
public class RestUpgradeSynchronizerCompatTest extends RestUpgradeSynchronizerTest {

   @Override
   protected ConfigurationBuilder getSourceServerBuilder() {
      ConfigurationBuilder sourceServerBuilder = super.getSourceServerBuilder();
      sourceServerBuilder.compatibility().enable();
      return sourceServerBuilder;
   }

}
