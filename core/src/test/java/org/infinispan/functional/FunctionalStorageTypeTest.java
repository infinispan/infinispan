package org.infinispan.functional;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.StorageType;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "functional.FunctionalStorageTypeTest")
public class FunctionalStorageTypeTest extends FunctionalMapTest {
   @Override
   protected void configureCache(ConfigurationBuilder builder) {
      builder.memory().storage(storageType);
      super.configureCache(builder);
   }

   public Object[] factory() {
      return new Object[]{
            new FunctionalStorageTypeTest().storageType(StorageType.OFF_HEAP),
            new FunctionalStorageTypeTest().storageType(StorageType.BINARY),
            new FunctionalStorageTypeTest().storageType(StorageType.HEAP),
      };
   }
}
