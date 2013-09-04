package org.infinispan.loaders.file;

import org.infinispan.configuration.cache.FileCacheStoreConfigurationBuilder;
import org.infinispan.loaders.bucket.Bucket;
import org.testng.annotations.Test;

import java.io.File;

@Test(groups = "unit", testName = "loaders.file.FileCacheStorePerWriteTest")
public class FileCacheStorePerWriteTest extends FileCacheStoreTest {

   @Override
   protected FileCacheStoreConfigurationBuilder.FsyncMode getFsyncMode() {
      return FileCacheStoreConfigurationBuilder.FsyncMode.PER_WRITE;
   }

   protected void checkBucketExists(Bucket b) {
      assert !new File(fcs.root, b.getBucketIdAsString()).exists();
   }

}
