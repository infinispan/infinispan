package org.infinispan.client.hotrod.query;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.StorageType;
import org.testng.annotations.Test;

/**
 * @author anistor@redhat.com
 */
@Test(groups = "functional", testName = "client.hotrod.query.RemoteOffHeapNonIndexedQueryDslConditionsTest")
public class RemoteOffHeapNonIndexedQueryDslConditionsTest extends RemoteNonIndexedQueryDslConditionsTest {

   @Override
   protected ConfigurationBuilder getConfigurationBuilder() {
      ConfigurationBuilder builder = super.getConfigurationBuilder();
      builder.memory().storage(StorageType.OFF_HEAP);
      return builder;
   }
}
