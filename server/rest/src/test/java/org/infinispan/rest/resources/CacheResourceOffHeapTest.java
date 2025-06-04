package org.infinispan.rest.resources;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.StorageType;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "rest.CacheResourceOffHeapTest")
public class CacheResourceOffHeapTest extends BaseCacheResourceTest {

   @Override
   public ConfigurationBuilder getDefaultCacheBuilder() {
      ConfigurationBuilder configurationBuilder = super.getDefaultCacheBuilder();
      configurationBuilder.memory().storage(StorageType.OFF_HEAP);
      return configurationBuilder;
   }

   @Override
   public Object[] factory() {
      return new Object[]{
            new CacheResourceOffHeapTest().withSecurity(false).browser(false),
            new CacheResourceOffHeapTest().withSecurity(false).browser(true),
            new CacheResourceOffHeapTest().withSecurity(true).browser(false),
            new CacheResourceOffHeapTest().withSecurity(true).browser(true),
      };
   }

}
