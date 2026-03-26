package org.infinispan.persistence.sifs;

import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

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
         assertNotNull("Expected getFile to succeed for existing empty file when isIndex is false", handle);

         if (handle != null) {
            handle.close();
         }
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
         assertNotNull("Expected getFile to succeed for empty file when isIndex is true", handle);

         if (handle != null) {
            handle.close();
         }
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
         assertNull("Expected getFile to return null for non-existent file when isIndex is false", handle);
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
         assertNotNull("Expected getFile to create and open file when isIndex is true", handle);

         if (handle != null) {
            handle.close();
         }
      } finally {
         fileProvider.stop();
      }
   }
}
