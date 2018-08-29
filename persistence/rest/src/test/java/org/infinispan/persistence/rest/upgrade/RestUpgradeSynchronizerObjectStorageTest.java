package org.infinispan.persistence.rest.upgrade;

import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OBJECT_TYPE;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.testng.annotations.Test;

@Test(testName = "persistence.rest.upgrade.RestUpgradeSynchronizerObjectStorageTest", groups = "functional")
public class RestUpgradeSynchronizerObjectStorageTest extends RestUpgradeSynchronizerTest {

   @Override
   protected ConfigurationBuilder getSourceServerBuilder() {
      ConfigurationBuilder sourceServerBuilder = super.getSourceServerBuilder();
      sourceServerBuilder.encoding().key().mediaType(APPLICATION_OBJECT_TYPE);
      sourceServerBuilder.encoding().value().mediaType(APPLICATION_OBJECT_TYPE);
      return sourceServerBuilder;
   }

}
