package org.infinispan.persistence.rocksdb;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import org.infinispan.commons.util.Util;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.FlushOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

/**
 * Verifies that flushing the metadata column family after writing metadata
 * allows RocksDB to reclaim WAL files.
 *
 * @see <a href="https://github.com/infinispan/infinispan/issues/17732">#17732</a>
 */
@Test(groups = "unit", testName = "persistence.rocksdb.RocksDBWalTest")
public class RocksDBWalTest {

   static {
      RocksDB.loadLibrary();
   }

   private final Path tmpDir = Path.of(System.getProperty("java.io.tmpdir"), "rocksdb-wal-test-" + System.nanoTime());

   @AfterMethod(alwaysRun = true)
   void cleanup() {
      Util.recursiveFileRemove(tmpDir);
   }

   public void testMetaCfFlushAllowsWalReclamation() throws RocksDBException, IOException {
      long walCountWithFlush = runWithMetaCfFlush(true);
      long walCountWithoutFlush = runWithMetaCfFlush(false);

      assertTrue(walCountWithFlush <= 2,
            "With meta-cf flush, expected at most 2 WAL files but found " + walCountWithFlush);
      assertTrue(walCountWithoutFlush > 2,
            "Without meta-cf flush, expected WAL file accumulation but found only " + walCountWithoutFlush);
   }

   private long runWithMetaCfFlush(boolean flushMetaCf) throws RocksDBException, IOException {
      Path dbPath = tmpDir.resolve(flushMetaCf ? "with-flush" : "without-flush");
      File dir = dbPath.toFile();
      dir.mkdirs();

      ColumnFamilyOptions dataCfOpts = new ColumnFamilyOptions()
            .setWriteBufferSize(4096);
      ColumnFamilyOptions metaCfOpts = new ColumnFamilyOptions();

      List<ColumnFamilyDescriptor> descriptors = new ArrayList<>();
      descriptors.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, dataCfOpts));
      descriptors.add(new ColumnFamilyDescriptor("meta-cf".getBytes(), metaCfOpts));

      List<ColumnFamilyHandle> handles = new ArrayList<>();
      DBOptions dbOptions = new DBOptions()
            .setCreateIfMissing(true)
            .setCreateMissingColumnFamilies(true);

      try (RocksDB db = RocksDB.open(dbOptions, dbPath.toString(), descriptors, handles)) {
         ColumnFamilyHandle defaultCf = handles.get(0);
         ColumnFamilyHandle metaCf = handles.get(1);

         // Simulate writeMetadata()
         db.put(metaCf, "metadata".getBytes(), "version-data".getBytes());

         if (flushMetaCf) {
            try (FlushOptions flushOptions = new FlushOptions().setWaitForFlush(true)) {
               db.flush(flushOptions, metaCf);
            }
         }

         // Write enough data to the default CF to trigger multiple memtable flushes
         byte[] value = new byte[1024];
         for (int i = 0; i < 500; i++) {
            db.put(defaultCf, ("key-" + i).getBytes(), value);
         }

         // Flush the data CF to allow WAL cleanup
         try (FlushOptions flushOptions = new FlushOptions().setWaitForFlush(true)) {
            db.flush(flushOptions, defaultCf);
         }

         for (ColumnFamilyHandle h : handles) {
            h.close();
         }
      }
      dbOptions.close();
      dataCfOpts.close();
      metaCfOpts.close();

      try (Stream<Path> files = Files.list(dbPath)) {
         return files.filter(p -> p.toString().endsWith(".log")).count();
      }
   }
}
