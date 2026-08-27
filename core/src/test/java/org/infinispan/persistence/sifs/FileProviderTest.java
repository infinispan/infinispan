package org.infinispan.persistence.sifs;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import org.infinispan.commons.util.Util;
import org.infinispan.testing.Testing;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "persistence.sifs.FileProviderTest")
public class FileProviderTest {

   private String tmpDirectory;

   @BeforeMethod
   protected void setUpTempDir() {
      tmpDirectory = Testing.tmpDirectory(getClass());
   }

   @AfterMethod
   protected void clearTempDir() {
      Util.recursiveFileRemove(tmpDirectory);
   }

   public void testGetFileReadsEmptyFileWhenIsIndexFalse() throws IOException {
      Path dataPath = Path.of(tmpDirectory, "data");
      FileProvider fileProvider = new FileProvider(dataPath, 10, "test-", 1000, false);

      try {
         // Create an empty file to simulate the scenario
         File emptyFile = fileProvider.newFile(1);
         emptyFile.createNewFile();

         // When isIndex is false, opening an existing empty file with "r" mode should succeed
         // (read-only mode can open existing empty files)
         FileProvider.Handle handle = fileProvider.getFile(1);
         assertNotNull(handle, "Expected getFile to succeed for existing empty file when isIndex is false");
         handle.close();
      } finally {
         fileProvider.stop();
      }
   }

   public void testGetFileSucceedsWhenIsIndexTrue() throws IOException {
      Path indexPath = Path.of(tmpDirectory, "index");
      FileProvider fileProvider = new FileProvider(indexPath, 10, "test-", 1000, true);

      try {
         // Create an empty file to simulate the scenario
         File emptyFile = fileProvider.newFile(1);
         emptyFile.createNewFile();

         // When isIndex is true, opening with "rw" mode should succeed even for empty files
         FileProvider.Handle handle = fileProvider.getFile(1);
         assertNotNull(handle, "Expected getFile to succeed for empty file when isIndex is true");

         handle.close();
      } finally {
         fileProvider.stop();
      }
   }

   public void testGetFileNonExistentFileWhenIsIndexFalse() throws IOException {
      Path dataPath = Path.of(tmpDirectory, "data");
      FileProvider fileProvider = new FileProvider(dataPath, 10, "test-", 1000, false);

      try {
         // When isIndex is false, opening a non-existent file with "r" mode should fail
         // getFile catches FileNotFoundException and returns null
         FileProvider.Handle handle = fileProvider.getFile(999);
         assertNull(handle, "Expected getFile to return null for non-existent file when isIndex is false");
      } finally {
         fileProvider.stop();
      }
   }

   public void testGetFileNonExistentFileWhenIsIndexTrue() throws IOException {
      Path indexPath = Path.of(tmpDirectory, "index");
      FileProvider fileProvider = new FileProvider(indexPath, 10, "test-", 1000, true);

      try {
         // When isIndex is true, opening with "rw" mode should create the file if it doesn't exist
         FileProvider.Handle handle = fileProvider.getFile(999);
         assertNotNull(handle, "Expected getFile to create and open file when isIndex is true");
         handle.close();
      } finally {
         fileProvider.stop();
      }
   }

   public void testClearDoesNotBlockWhileHandleOpen() throws Exception {
      Path indexPath = Path.of(tmpDirectory, "index");
      FileProvider fileProvider = new FileProvider(indexPath, 10, "test-", 1000, true);

      try {
         // Open a file and keep the handle open across the clear()
         FileProvider.Handle handle = fileProvider.getFile(1);
         assertNotNull(handle);
         File dataFile = fileProvider.newFile(1);
         assertTrue(dataFile.exists(), "data file should exist while the handle is open");

         // clear() must return promptly even though a handle is still open. The previous
         // implementation busy-spun on a CPU core until the handle was released (pinning the carrier
         // when run on a virtual thread), which could hang the JVM indefinitely.
         Thread clearThread = new Thread(() -> {
            try {
               fileProvider.clear();
            } catch (IOException e) {
               throw new UncheckedIOException(e);
            }
         }, "test-clear");
         clearThread.start();
         clearThread.join(TimeUnit.SECONDS.toMillis(30));
         assertFalse(clearThread.isAlive(), "clear() must not block or busy-spin while a handle is open");

         // Deletion of the in-use file is deferred until its last handle is released.
         assertTrue(dataFile.exists(), "in-use file should not be deleted until the handle is closed");
         handle.close();
         assertFalse(dataFile.exists(), "file should be deleted once the last handle is closed");
      } finally {
         fileProvider.stop();
      }
   }
}
