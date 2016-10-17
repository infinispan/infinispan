package org.infinispan.stream;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.StorageType;
import org.testng.annotations.Test;

/**
 * @author William Burns
 */
@Test(groups = "functional", testName = "streams.SimpleStreamOffHeapTest")
public class SimpleStreamOffHeapTest extends SimpleStreamTest {
   @Override
   protected void enhanceConfiguration(ConfigurationBuilder builder) {
      super.enhanceConfiguration(builder);
      builder.memory().storageType(StorageType.OFF_HEAP);
   }
}
