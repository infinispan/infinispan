package org.infinispan.persistence.sifs;

import static org.testng.AssertJUnit.assertNull;

import org.infinispan.commons.test.CommonsTestingUtil;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.persistence.BaseNonBlockingStoreTest;
import org.infinispan.persistence.spi.MarshallableEntry;
import org.infinispan.persistence.spi.NonBlockingStore;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "persistence.sifs.SoftIndexFileStoreTest")
public class SoftIndexFileStoreTest extends BaseNonBlockingStoreTest {

   protected String tmpDirectory;

   @BeforeClass(alwaysRun = true)
   protected void setUpTempDir() {
      tmpDirectory = CommonsTestingUtil.tmpDirectory(getClass());
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
      return configurationBuilder.persistence()
            .addSoftIndexFileStore()
            .dataLocation(CommonsTestingUtil.tmpDirectory(tmpDirectory, "data"))
            .indexLocation(CommonsTestingUtil.tmpDirectory(tmpDirectory, "index"))
            .maxFileSize(1000)
            .build();
   }

   // test for ISPN-5753
   public void testOverrideWithExpirableAndCompaction() throws InterruptedException {
      // write immortal entry
      store.write(marshalledEntry(internalCacheEntry("key", "value1", -1)));
      writeGibberish(); // make sure that compaction happens - value1 is compacted
      store.write(marshalledEntry(internalCacheEntry("key", "value2", 1)));
      timeService.advance(2);
      writeGibberish(); // make sure that compaction happens - value2 expires
      store.stop();
      startStore(store);
      // value1 has been overwritten and value2 has expired
      MarshallableEntry entry = store.loadEntry("key");
      assertNull(entry != null ? entry.getKey() + "=" + entry.getValue() : null, entry);
   }

   private void writeGibberish() {
      for (int i = 0; i < 100; ++i) {
         store.write(marshalledEntry(internalCacheEntry("foo", "bar", -1)));
         store.delete("foo");
      }
   }
}
