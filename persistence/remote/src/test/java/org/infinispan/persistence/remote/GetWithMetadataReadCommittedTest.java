package org.infinispan.persistence.remote;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.annotations.Test;

/**
 * @author gustavonalle
 * @since 9.0
 */
@Test(testName = "persistence.remote.GetWithMetadataReadCommittedTest", groups = "functional")
public class GetWithMetadataReadCommittedTest extends GetWithMetadataTest {

   @Override
   protected ConfigurationBuilder getTargetCacheConfiguration(int sourcePort) {
      ConfigurationBuilder targetCacheConfiguration = super.getTargetCacheConfiguration(sourcePort);
      targetCacheConfiguration.locking().isolationLevel(IsolationLevel.READ_COMMITTED);
      return targetCacheConfiguration;
   }
}
