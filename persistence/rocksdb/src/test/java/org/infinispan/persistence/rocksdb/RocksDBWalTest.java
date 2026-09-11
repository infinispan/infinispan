package org.infinispan.persistence.rocksdb;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.marshall.TestObjectStreamMarshaller;
import org.infinispan.marshall.persistence.impl.MarshalledEntryUtil;
import org.infinispan.persistence.rocksdb.configuration.RocksDBStoreConfigurationBuilder;
import org.infinispan.persistence.support.EnsureNonBlockingStore;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestDataSCI;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.test.fwk.TestInternalCacheEntryFactory;
import org.infinispan.testing.Testing;
import org.infinispan.util.PersistenceMockUtil;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

/**
 * Verifies that {@link RocksDBStore} flushes the metadata column family after writing metadata,
 * which allows RocksDB to reclaim WAL files.
 *
 * @see <a href="https://github.com/infinispan/infinispan/issues/17732">#17732</a>
 */
@Test(groups = "unit", testName = "persistence.rocksdb.RocksDBWalTest")
public class RocksDBWalTest extends AbstractInfinispanTest {

   static {
      // Pre-load the native library before BlockHound is active so that the one-time
      // System.loadLibrary() call inside RocksDB.loadLibrary() is not intercepted as
      // a blocking operation during store start.
      org.rocksdb.RocksDB.loadLibrary();
   }

   private final String tmpDirectory = Testing.tmpDirectory(this.getClass());

   @AfterClass(alwaysRun = true)
   protected void clearTempDir() {
      Util.recursiveFileRemove(tmpDirectory);
   }

   /**
    * Starts the store, writes enough data to force many data CF memtable flushes (so the meta-cf
    * write is in an older WAL that the data CF has already flushed past), then checks that the
    * number of live WAL files is small. Without the meta-cf flush in the writeMetadata method
    * the meta-cf's unflushed sequence number pins the old WAL file and it cannot be reclaimed, so
    * WAL files accumulate without bound.
    */
   public void testWriteMetadataFlushesMetaCfAllowingWalReclamation() throws Exception {
      ConfigurationBuilder builder = TestCacheManagerFactory.getDefaultCacheConfiguration(false);
      builder.persistence().addStore(RocksDBStoreConfigurationBuilder.class)
            .segmented(false)
            .location(tmpDirectory)
            .expiredLocation(tmpDirectory)
            // Tiny write buffer on the data CF forces many automatic memtable flushes while
            // writing, so the WAL file containing the meta-cf write (from start()) is left far
            // behind. Without flushing the meta-cf, that WAL stays pinned.
            .addProperty(RocksDBStore.COLUMN_FAMILY_PROPERTY_NAME_WITH_SUFFIX + "write_buffer_size", "4096");
      Configuration configuration = builder.build();

      TestObjectStreamMarshaller marshaller = new TestObjectStreamMarshaller(TestDataSCI.INSTANCE);
      PersistenceMockUtil.InvocationContextBuilder ctxBuilder =
            new PersistenceMockUtil.InvocationContextBuilder(getClass(), configuration, marshaller);

      EnsureNonBlockingStore<Object, Object> store =
            new EnsureNonBlockingStore<>(new RocksDBStore<>(), k -> Math.abs(k.hashCode() % 256));
      store.startAndWait(ctxBuilder.build());

      // Write enough data to trigger many automatic data-CF memtable flushes.
      // With a 4096-byte write buffer each ~1 KB entry nearly fills one memtable,
      // so 500 writes yield ~500 flushes, leaving the meta-cf write far behind in WAL history.
      for (int i = 0; i < 500; i++) {
         InternalCacheEntry entry = TestInternalCacheEntryFactory.create("key-" + i, new byte[1024]);
         store.write(MarshalledEntryUtil.create(entry, marshaller));
      }

      // Count WAL files while the store is still open (before close flushes everything).
      // If meta-cf was flushed by writeMetadata(), RocksDB can reclaim WAL files as the
      // data CF flushes them. If not, the early WAL containing the meta-cf write stays pinned.
      Path dataDir = Paths.get(Testing.tmpDirectory(getClass()), "mock-cache", "data");
      long walCount;
      try (Stream<Path> files = Files.list(dataDir)) {
         walCount = files.filter(p -> p.toString().endsWith(".log")).count();
      }

      store.stopAndWait();
      marshaller.stop();

      assertTrue(walCount <= 2, "Expected at most 2 WAL files after writeMetadata() flush, but found " + walCount);
   }
}
