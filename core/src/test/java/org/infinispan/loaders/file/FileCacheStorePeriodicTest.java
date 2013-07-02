package org.infinispan.loaders.file;

import org.testng.annotations.Test;

@Test(groups = "unit", testName = "loaders.file.FileCacheStorePeriodicTest")
public class FileCacheStorePeriodicTest extends FileCacheStoreTest {

   @Override
   protected FileCacheStoreConfig.FsyncMode getFsyncMode() {
      return FileCacheStoreConfig.FsyncMode.PERIODIC;
   }

}
