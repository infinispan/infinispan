/*
 * Copyright 2011 Red Hat, Inc. and/or its affiliates.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA
 */

package org.infinispan.io;

import org.infinispan.Cache;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import static org.testng.Assert.*;
import static org.testng.Assert.assertEquals;

@Test(testName = "io.GridFileTest", groups = "functional")
public class GridFileTest extends SingleCacheManagerTest {

   private Cache<String, byte[]> dataCache;
   private Cache<String, GridFile.Metadata> metadataCache;
   private GridFilesystem fs;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      return new DefaultCacheManager();
   }

   @BeforeMethod
   protected void setUp() throws Exception {
      dataCache = cacheManager.getCache("data");
      metadataCache = cacheManager.getCache("metadata");
      fs = new GridFilesystem(dataCache, metadataCache);
   }

   public void testGridFS() throws IOException {
      File gridDir = fs.getFile("/test");
      assert gridDir.mkdirs();
      File gridFile = fs.getFile("/test/myfile.txt");
      assert gridFile.createNewFile();
   }

   public void testCreateNewFile() throws IOException {
      File file = fs.getFile("file.txt");
      assertTrue(file.createNewFile());   // file should be created successfully
      assertFalse(file.createNewFile());  // file should not be created, because it already exists
   }

   @Test(expectedExceptions = IOException.class)
   public void testCreateNewFileInNonExistentDir() throws IOException {
      File file = fs.getFile("nonExistent/file.txt");
      file.createNewFile();
   }

   public void testNonExistentFileIsNeitherFileNorDirectory() throws IOException {
      File file = fs.getFile("nonExistentFile.txt");
      assertFalse(file.exists());
      assertFalse(file.isFile());
      assertFalse(file.isDirectory());
   }

   public void testMkdir() throws IOException {
      assertFalse(mkdir(""));
      assertFalse(mkdir("/"));
      assertFalse(mkdir("/nonExistentParentDir/subDir"));
      assertTrue(mkdir("myDir1"));
      assertTrue(mkdir("myDir1/mySubDir1"));
      assertTrue(mkdir("/myDir2"));
      assertTrue(mkdir("/myDir2/mySubDir2"));

      fs.getFile("/file.txt").createNewFile();
      assertFalse(mkdir("/file.txt/dir"));
   }

   private boolean mkdir(String pathname) {
      return fs.getFile(pathname).mkdir();
   }

   public void testMkdirs() throws IOException {
      assertFalse(mkdirs(""));
      assertFalse(mkdirs("/"));
      assertTrue(mkdirs("myDir1"));
      assertTrue(mkdirs("myDir2/mySubDir"));
      assertTrue(mkdirs("/myDir3"));
      assertTrue(mkdirs("/myDir4/mySubDir"));
      assertTrue(mkdirs("/myDir5/subDir/secondSubDir"));

      fs.getFile("/file.txt").createNewFile();
      assertFalse(mkdirs("/file.txt/dir"));
   }

   private boolean mkdirs(String pathname) {
      return fs.getFile(pathname).mkdirs();
   }

   public void testGetParent() throws IOException {
      File file = fs.getFile("file.txt");
      assertEquals(file.getParent(), null);

      file = fs.getFile("/parentdir/file.txt");
      assertEquals(file.getParent(), "/parentdir");

      file = fs.getFile("/parentdir/subdir/file.txt");
      assertEquals(file.getParent(), "/parentdir/subdir");
   }

   public void testGetParentFile() throws IOException {
      File file = fs.getFile("file.txt");
      assertNull(file.getParentFile());

      file = fs.getFile("/parentdir/file.txt");
      File parentDir = file.getParentFile();
      assertTrue(parentDir instanceof GridFile);
      assertEquals(parentDir.getPath(), "/parentdir");
   }

   @Test(expectedExceptions = FileNotFoundException.class)
   public void testWritingToDirectoryThrowsException1() throws IOException {
      GridFile dir = (GridFile) createDir();
      fs.getOutput(dir);  // should throw exception
   }

   @Test(expectedExceptions = FileNotFoundException.class)
   public void testWritingToDirectoryThrowsException2() throws IOException {
      File dir = createDir();
      fs.getOutput(dir.getPath());  // should throw exception
   }

   @Test(expectedExceptions = FileNotFoundException.class)
   public void testReadingFromDirectoryThrowsException1() throws IOException {
      File dir = createDir();
      fs.getInput(dir);  // should throw exception
   }

   @Test(expectedExceptions = FileNotFoundException.class)
   public void testReadingFromDirectoryThrowsException2() throws IOException {
      File dir = createDir();
      fs.getInput(dir.getPath());  // should throw exception
   }

   private File createDir() {
      File dir = fs.getFile("mydir");
      boolean created = dir.mkdir();
      assert created;
      return dir;
   }

   public void testWriteAcrossMultipleChunksWithNonDefaultChunkSize() throws Exception {
      writeToFile("multipleChunks.txt",
                  "This text spans multiple chunks, because each chunk is only 10 bytes long.",
                  10);  // chunkSize = 10

      String text = getContents("multipleChunks.txt");
      assertEquals(text, "This text spans multiple chunks, because each chunk is only 10 bytes long.");
   }

   public void testWriteAcrossMultipleChunksWithNonDefaultChunkSizeAfterFileIsExplicitlyCreated() throws Exception {
      GridFile file = (GridFile) fs.getFile("multipleChunks.txt", 20);  // chunkSize = 20
      file.createNewFile();

      writeToFile("multipleChunks.txt",
                  "This text spans multiple chunks, because each chunk is only 20 bytes long.",
                  10);  // chunkSize = 10 (but it is ignored, because the file was already created with chunkSize = 20

      String text = getContents("multipleChunks.txt");
      assertEquals(text, "This text spans multiple chunks, because each chunk is only 20 bytes long.");
   }

   public void testAppend() throws Exception {
      writeToFile("append.txt", "Hello");
      appendToFile("append.txt", "World");
      assertEquals(getContents("append.txt"), "HelloWorld");
   }

   public void testAppendWithDifferentChunkSize() throws Exception {
      writeToFile("append.txt", "Hello", 2);   // chunkSize = 2
      appendToFile("append.txt", "World", 5);        // chunkSize = 5
      assertEquals(getContents("append.txt"), "HelloWorld");
   }

   public void testAppendToEmptyFile() throws Exception {
      appendToFile("empty.txt", "Hello");
      assertEquals(getContents("empty.txt"), "Hello");
   }

   public void testDeleteRemovesAllChunks() throws Exception {
      assertEquals(numberOfChunksInCache(), 0);
      assertEquals(numberOfMetadataEntries(), 0);

      writeToFile("delete.txt", "delete me", 100);

      GridFile file = (GridFile) fs.getFile("delete.txt");
      boolean deleted = file.delete(true);
      assertTrue(deleted);
      assertFalse(file.exists());
      assertEquals(numberOfChunksInCache(), 0);
      assertEquals(numberOfMetadataEntries(), 0);
   }

   public void testOverwritingFileDoesNotLeaveExcessChunksInCache() throws Exception {
      assertEquals(numberOfChunksInCache(), 0);

      writeToFile("leak.txt", "12345abcde12345", 5); // file length = 15, chunkSize = 5
      assertEquals(numberOfChunksInCache(), 3);

      writeToFile("leak.txt", "12345", 5);           // file length = 5, chunkSize = 5
      assertEquals(numberOfChunksInCache(), 1);
   }

   private int numberOfChunksInCache() {
      return dataCache.size();
   }

   private int numberOfMetadataEntries() {
      return metadataCache.size();
   }

   private void appendToFile(String filePath, String text) throws IOException {
      appendToFile(filePath, text, null);
   }

   private void appendToFile(String filePath, String text, Integer chunkSize) throws IOException {
      writeToFile(filePath, text, true, chunkSize);
   }

   private void writeToFile(String filePath, String text) throws IOException {
      writeToFile(filePath, text, null);
   }

   private void writeToFile(String filePath, String text, Integer chunkSize) throws IOException {
      writeToFile(filePath, text, false, chunkSize);
   }

   private void writeToFile(String filePath, String text, boolean append, Integer chunkSize) throws IOException {
      OutputStream out = chunkSize == null
         ? fs.getOutput(filePath, append)
         : fs.getOutput(filePath, append, chunkSize);
      try {
         out.write(text.getBytes());
      } finally {
         out.close();
      }
   }

   private String getContents(String filePath) throws IOException {
      InputStream in = fs.getInput(filePath);
      try {
         byte[] buf = new byte[1000];
         int bytesRead = in.read(buf);
         return new String(buf, 0, bytesRead);
      } finally {
         in.close();
      }
   }

}
