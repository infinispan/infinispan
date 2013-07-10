package org.infinispan.loaders.file;

import org.infinispan.configuration.cache.FileCacheStoreConfigurationBuilder;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "loaders.file.FileCacheStorePeriodicTest")
public class FileCacheStorePeriodicTest extends FileCacheStoreTest {

   @Override
   protected FileCacheStoreConfigurationBuilder.FsyncMode getFsyncMode() {
      return FileCacheStoreConfigurationBuilder.FsyncMode.PERIODIC;
   }

}
