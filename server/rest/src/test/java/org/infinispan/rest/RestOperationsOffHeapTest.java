package org.infinispan.rest;

import org.infinispan.commons.dataconversion.Encoder;
import org.infinispan.commons.dataconversion.UTF8Encoder;
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
   protected Class<? extends Encoder> getKeyEncoding() {
      return UTF8Encoder.class;
   }
}
