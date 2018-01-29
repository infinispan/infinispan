package org.infinispan.rest;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.StorageType;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "rest.RestOperationsOffHeapTest")
public class RestOperationsOffHeapTest extends BaseRestOperationsTest {

   @Override
   public ConfigurationBuilder getDefaultCacheBuilder() {
      ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();
      configurationBuilder.memory().storageType(StorageType.OFF_HEAP);
      return configurationBuilder;
   }

   @Override
   protected boolean enableCompatibility() {
      return false;
   }

}
