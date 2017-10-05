package org.infinispan.functional;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.StorageType;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "functional.FunctionalEncoderTest")
public class FunctionalStorageTypeTest extends FunctionalMapTest {
   StorageType storageType;

   @Override
   protected void configureCache(ConfigurationBuilder builder) {
      builder.memory().storageType(storageType);
      super.configureCache(builder);
   }

   public Object[] factory() {
      return new Object[]{
            new FunctionalStorageTypeTest().storageType(StorageType.OFF_HEAP),
            new FunctionalStorageTypeTest().storageType(StorageType.BINARY),
            new FunctionalStorageTypeTest().storageType(StorageType.OBJECT),
      };
   }

   FunctionalStorageTypeTest storageType(StorageType storageType) {
      this.storageType = storageType;
      return this;
   }
}
