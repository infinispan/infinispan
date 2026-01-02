package org.infinispan.persistence.sifs;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.fail;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;

import org.infinispan.commons.test.CommonsTestingUtil;
import org.infinispan.commons.util.Util;
import org.infinispan.commons.util.concurrent.CompletionStages;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.configuration.cache.StoreConfigurationBuilder;
import org.infinispan.distribution.BaseDistStoreTest;
import org.infinispan.encoding.DataConversion;
import org.infinispan.persistence.spi.MarshallableEntry;
import org.infinispan.persistence.spi.MarshallableEntryFactory;
import org.infinispan.persistence.support.WaitDelegatingNonBlockingStore;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "persistence.sifs.SoftIndexFileStoreCorruptRestartTest")
public class SoftIndexFileStoreCorruptRestartTest extends BaseDistStoreTest<Integer, String, SoftIndexFileStoreRestartTest> {
   protected String tmpDirectory;

   {
      // We don't really need a cluster
      INIT_CLUSTER_SIZE = 1;
      l1CacheEnabled = false;
      segmented = true;
   }

   @Override
   public Object[] factory() {
      return new Object[] {
            new SoftIndexFileStoreCorruptRestartTest().cacheMode(CacheMode.LOCAL),
            new SoftIndexFileStoreCorruptRestartTest().cacheMode(CacheMode.DIST_SYNC),
      };
   }

   @BeforeClass(alwaysRun = true)
   @Override
   public void createBeforeClass() throws Throwable {
      tmpDirectory = CommonsTestingUtil.tmpDirectory(getClass());
      super.createBeforeClass();
   }


   @AfterClass(alwaysRun = true)
   @Override
   protected void destroy() {
      super.destroy();
      Util.recursiveFileRemove(tmpDirectory);
   }

   @Override
   protected StoreConfigurationBuilder addStore(PersistenceConfigurationBuilder persistenceConfigurationBuilder, boolean shared) {
      // We don't support shared for SIFS
      assert !shared;
      return persistenceConfigurationBuilder.addSoftIndexFileStore()
            .dataLocation(Paths.get(tmpDirectory, "data").toString())
            .indexLocation(Paths.get(tmpDirectory, "index").toString());
   }

   @Test(dataProvider = "booleans")
   public void testRestartWithEmptyDataFile(boolean deleteIndex) throws Throwable {
      cache(0, cacheName).put("some-value", "1");
      assertEquals(1, cache(0, cacheName).size());

      killMember(0, cacheName);

      if (deleteIndex) {
         // Delete the index which should force it to rebuild
         Util.recursiveFileRemove(Paths.get(tmpDirectory, "index"));
      }

      File file = Paths.get(tmpDirectory, "data", cacheName, "data", "ispn12.123").toFile();
      if (!file.createNewFile()) {
         fail("Unable to create file: " + file);
      }

      createCacheManagers();

      WaitDelegatingNonBlockingStore store = TestingUtil.getFirstStoreWait(cache(0, cacheName));

      Compactor compactor = TestingUtil.extractField(store.delegate(), "compactor");

      // Force compaction for the previous file
      CompletionStages.join(compactor.forceCompactionForAllNonLogFiles());

      assertEquals(1, cache(0, cacheName).size());
   }

   MarshallableEntry objectToMarshalledBuffer(Object obj) {
      DataConversion dc = cache(0, cacheName).getAdvancedCache().getKeyDataConversion();
      Object storedKey = dc.toStorage(obj);

      MarshallableEntryFactory mef = TestingUtil.extractComponent(cache(0, cacheName), MarshallableEntryFactory.class);

      return mef.create(storedKey);
   }

   @DataProvider(name = "booleans")
   Object[][] booleans() {
      return new Object[][]{
            {Boolean.TRUE}, {Boolean.FALSE}};
   }

   @Test(dataProvider = "booleans")
   public void testRestartWithPartialKeyWritten(boolean deleteIndex) throws Throwable {
      cache(0, cacheName).put("some-value", "1");
      assertEquals(1, cache(0, cacheName).size());

      org.infinispan.commons.io.ByteBuffer keyBytes = objectToMarshalledBuffer("key-1").getKeyBytes();

      ByteBuffer trimmedKey = ByteBuffer.wrap(keyBytes.getBuf(), 0, keyBytes.getLength() - 1);

      killMember(0, cacheName);

      if (deleteIndex) {
         // Delete the index which should force it to rebuild
         Util.recursiveFileRemove(Paths.get(tmpDirectory, "index"));
      }

      File file = Paths.get(tmpDirectory, "data", cacheName, "data", "ispn12.123").toFile();
      if (!file.createNewFile()) {
         fail("Unable to create file: " + file);
      }

      try (FileOutputStream stream = new FileOutputStream(file)) {
         FileChannel channel = stream.getChannel();
         ByteBuffer buffer = ByteBuffer.allocate(EntryHeader.HEADER_SIZE_11_0);
         EntryHeader.writeHeader(buffer, (short) (trimmedKey.remaining() + 2), (short) 0, 0, (short) 0, 20L, -1L);
         buffer.flip();
         int writeCount = channel.write(buffer);
         assertEquals(EntryHeader.HEADER_SIZE_11_0, writeCount);

         int expected = trimmedKey.remaining();
         assertEquals(expected, channel.write(trimmedKey));
      }

      createCacheManagers();

      WaitDelegatingNonBlockingStore store = TestingUtil.getFirstStoreWait(cache(0, cacheName));

      Compactor compactor = TestingUtil.extractField(store.delegate(), "compactor");

      // Force compaction for the previous file
      CompletionStages.join(compactor.forceCompactionForAllNonLogFiles());

      assertEquals(1, cache(0, cacheName).size());
   }

   @Test(dataProvider = "booleans")
   public void testRestartWithOnlyHeaderWritten(boolean deleteIndex) throws Throwable {
      cache(0, cacheName).put("some-value", "1");
      assertEquals(1, cache(0, cacheName).size());

      killMember(0, cacheName);

      if (deleteIndex) {
         // Delete the index which should force it to rebuild
         Util.recursiveFileRemove(Paths.get(tmpDirectory, "index"));
      }

      File file = Paths.get(tmpDirectory, "data", cacheName, "data", "ispn12.123").toFile();
      if (!file.createNewFile()) {
         fail("Unable to create file: " + file);
      }

      try (FileOutputStream stream = new FileOutputStream(file)) {
         FileChannel channel = stream.getChannel();
         ByteBuffer buffer = ByteBuffer.allocate(EntryHeader.HEADER_SIZE_11_0);
         EntryHeader.writeHeader(buffer, (short) 10, (short) 0, 100, (short) 0, 20L, -1L);
         buffer.flip();
         int writeCount = channel.write(buffer);
         assertEquals(EntryHeader.HEADER_SIZE_11_0, writeCount);
      }

      createCacheManagers();

      WaitDelegatingNonBlockingStore store = TestingUtil.getFirstStoreWait(cache(0, cacheName));

      Compactor compactor = TestingUtil.extractField(store.delegate(), "compactor");

      // Force compaction for the previous file
      CompletionStages.join(compactor.forceCompactionForAllNonLogFiles());

      assertEquals(1, cache(0, cacheName).size());
   }
}
