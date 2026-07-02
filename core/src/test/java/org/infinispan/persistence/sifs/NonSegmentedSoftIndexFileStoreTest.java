package org.infinispan.persistence.sifs;

import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.persistence.BaseNonBlockingStoreTest;
import org.infinispan.persistence.spi.NonBlockingStore;
import org.infinispan.test.TestingUtil;
import org.infinispan.testing.Testing;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "persistence.sifs.NonSegmentedSoftIndexFileStoreTest")
public class NonSegmentedSoftIndexFileStoreTest extends BaseNonBlockingStoreTest {

   protected String tmpDirectory;

   @BeforeClass(alwaysRun = true)
   protected void setUpTempDir() {
      tmpDirectory = Testing.tmpDirectory(getClass());
   }

   @AfterClass(alwaysRun = true)
   protected void clearTempDir() {
      Util.recursiveFileRemove(tmpDirectory);
   }

   @Override
   protected NonBlockingStore createStore() {
      return new NonBlockingSoftIndexFileStore();
   }

   @Override
   protected Configuration buildConfig(ConfigurationBuilder configurationBuilder) {
      configurationBuilder.clustering().hash().numSegments(2);
      return configurationBuilder.persistence()
            .addSoftIndexFileStore()
            .segmented(false)
            .dataLocation(Paths.get(tmpDirectory, "data").toString())
            .indexLocation(Paths.get(tmpDirectory, "index").toString())
            .maxFileSize(1000)
            .build();
   }

   public void testCompactionWithNonSegmentedStore() throws Exception {
      for (int i = 0; i < 100; ++i) {
         store.write(marshalledEntry(internalCacheEntry("key-" + i, "value-" + i, -1)));
      }
      for (int i = 0; i < 100; ++i) {
         store.write(marshalledEntry(internalCacheEntry("key-" + i, "updated-" + i, -1)));
      }

      Compactor compactor = TestingUtil.extractField(store.delegate(), "compactor");
      compactor.forceCompactionForAllNonLogFiles()
            .toCompletableFuture().get(30, TimeUnit.SECONDS);
   }
}
