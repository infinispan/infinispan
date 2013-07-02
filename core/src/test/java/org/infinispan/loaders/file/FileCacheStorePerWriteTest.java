package org.infinispan.loaders.file;

import org.infinispan.loaders.bucket.Bucket;
import org.testng.annotations.Test;

import java.io.File;

@Test(groups = "unit", testName = "loaders.file.FileCacheStorePerWriteTest")
public class FileCacheStorePerWriteTest extends FileCacheStoreTest {

   @Override
   protected FileCacheStoreConfig.FsyncMode getFsyncMode() {
      return FileCacheStoreConfig.FsyncMode.PER_WRITE;
   }

   protected void checkBucketExists(Bucket b) {
      assert !new File(fcs.root, b.getBucketIdAsString()).exists();
   }

}
