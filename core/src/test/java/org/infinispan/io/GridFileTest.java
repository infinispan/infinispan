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
import java.io.FileFilter;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

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

   public void testGetFile() throws Exception {
      assertEquals(fs.getFile("file.txt").getPath(), "file.txt");
      assertEquals(fs.getFile("/file.txt").getPath(), "/file.txt");
      assertEquals(fs.getFile("myDir/file.txt").getPath(), "myDir/file.txt");
      assertEquals(fs.getFile("/myDir/file.txt").getPath(), "/myDir/file.txt");

      assertEquals(fs.getFile("myDir", "file.txt").getPath(), "myDir/file.txt");
      assertEquals(fs.getFile("/myDir", "file.txt").getPath(), "/myDir/file.txt");

      File dir = fs.getFile("/myDir");
      assertEquals(fs.getFile(dir, "file.txt").getPath(), "/myDir/file.txt");

      dir = fs.getFile("myDir");
      assertEquals(fs.getFile(dir, "file.txt").getPath(), "myDir/file.txt");
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

      createFile("/file.txt");
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

      createFile("/file.txt");
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
      return createDir("mydir");
   }

   private File createDir(String pathname) {
      File dir = fs.getFile(pathname);
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

   public void testSkipAndAvailable() throws Exception {
        String filePath = "skip.dat";
        OutputStream out = fs.getOutput(filePath);
        try{
            out.write(1);
            out.write(2);
            out.write(3);
        }finally{
            out.close();
        }
        InputStream in = fs.getInput(filePath);
        try{
            assertTrue(in.available()>=0);
            long skipped = in.skip(2);
            assertEquals(skipped, 2);
            assertEquals(in.read(), 3);
        }finally{
            in.close();
        }
    }

    public void testLastModified() throws Exception {
      assertEquals(fs.getFile("nonExistentFile.txt").lastModified(), 0);

      long time1 = System.currentTimeMillis();
      File file = createFile("file.txt");
      long time2 = System.currentTimeMillis();

      assertTrue(time1 <= file.lastModified());
      assertTrue(file.lastModified() <= time2);

      Thread.sleep(100);

      time1 = System.currentTimeMillis();
      writeToFile(file.getPath(), "foo");
      time2 = System.currentTimeMillis();

      assertTrue(time1 <= file.lastModified());
      assertTrue(file.lastModified() <= time2);
   }

   public void testList() throws Exception {
      assertNull(fs.getFile("nonExistentDir").list());
      assertEquals(createDir("/emptyDir").list().length, 0);

      File dir = createDirWithFiles();
      String[] filenames = dir.list();
      assertEquals(
            asSet(filenames),
            asSet("foo1.txt", "foo2.txt", "bar1.txt", "bar2.txt", "fooDir", "barDir"));
   }

   public void testListWithFilenameFilter() throws Exception {
      File dir = createDirWithFiles();
      String[] filenames = dir.list(new FooFilenameFilter());
      assertEquals(
            asSet(filenames),
            asSet("foo1.txt", "foo2.txt", "fooDir"));
   }

   public void testListFiles() throws Exception {
      assertNull(fs.getFile("nonExistentDir").listFiles());
      assertEquals(createDir("/emptyDir").listFiles().length, 0);

      File dir = createDirWithFiles();
      File[] files = dir.listFiles();
      assertEquals(
            asSet(getPaths(files)),
            asSet("/myDir/foo1.txt", "/myDir/foo2.txt", "/myDir/fooDir",
                  "/myDir/bar1.txt", "/myDir/bar2.txt", "/myDir/barDir"));
   }

   public void testListFilesWithFilenameFilter() throws Exception {
      File dir = createDirWithFiles();
      FooFilenameFilter filter = new FooFilenameFilter();
      filter.expectDir(dir);
      File[] files = dir.listFiles(filter);
      assertEquals(
            asSet(getPaths(files)),
            asSet("/myDir/foo1.txt", "/myDir/foo2.txt", "/myDir/fooDir"));
   }

   public void testListFilesWithFileFilter() throws Exception {
      File dir = createDirWithFiles();
      File[] files = dir.listFiles(new FooFileFilter());
      assertEquals(
            asSet(getPaths(files)),
            asSet("/myDir/foo1.txt", "/myDir/foo2.txt", "/myDir/fooDir"));
   }

   public void testRootDir() throws Exception {
      File rootDir = fs.getFile("/");
      assertTrue(rootDir.exists());
      assertTrue(rootDir.isDirectory());

      createFile("/foo.txt");
      String[] filenames = rootDir.list();
      assertNotNull(filenames);
      assertEquals(filenames.length, 1);
      assertEquals(filenames[0], "foo.txt");
   }

   public void testReadableChannel() throws Exception {
      String content = "This is the content of channelTest.txt";
      writeToFile("/channelTest.txt", content, 10);

      ReadableGridFileChannel channel = fs.getReadableChannel("/channelTest.txt");
      try {
         assertTrue(channel.isOpen());
         ByteBuffer buffer = ByteBuffer.allocate(1000);
         channel.read(buffer);
         assertEquals(getStringFrom(buffer), content);
      } finally {
         channel.close();
      }

      assertFalse(channel.isOpen());
   }

   public void testReadableChannelPosition() throws Exception {
      writeToFile("/position.txt", "0123456789", 3);

      ReadableGridFileChannel channel = fs.getReadableChannel("/position.txt");
      try {
         assertEquals(channel.position(), 0);

         channel.position(5);
         assertEquals(channel.position(), 5);
         assertEquals(getStringFromChannel(channel, 3), "567");
         assertEquals(channel.position(), 8);

         channel.position(2);
         assertEquals(channel.position(), 2);
         assertEquals(getStringFromChannel(channel, 5), "23456");
         assertEquals(channel.position(), 7);
      } finally {
         channel.close();
      }
   }

   public void testWritableChannel() throws Exception {
      WritableGridFileChannel channel = fs.getWritableChannel("/channelTest.txt", false, 10);
      try {
         assertTrue(channel.isOpen());
         channel.write(ByteBuffer.wrap("This file spans multiple chunks.".getBytes()));
      } finally {
         channel.close();
      }
      assertFalse(channel.isOpen());
      assertEquals(getContents("/channelTest.txt"), "This file spans multiple chunks.");
   }

   public void testWritableChannelAppend() throws Exception {
      writeToFile("/append.txt", "Initial text.", 3);

      WritableGridFileChannel channel = fs.getWritableChannel("/append.txt", true);
      try {
         channel.write(ByteBuffer.wrap("Appended text.".getBytes()));
      } finally {
         channel.close();
      }
      assertEquals(getContents("/append.txt"), "Initial text.Appended text.");
   }

   public void testGetAbsolutePath() throws IOException {
      assertEquals(fs.getFile("/file.txt").getAbsolutePath(), "/file.txt");
      assertEquals(fs.getFile("file.txt").getAbsolutePath(), "/file.txt");
      assertEquals(fs.getFile("dir/file.txt").getAbsolutePath(), "/dir/file.txt");
   }

   public void testGetAbsoluteFile() throws IOException {
      assertTrue(fs.getFile("file.txt").getAbsoluteFile() instanceof GridFile);
      assertEquals(fs.getFile("/file.txt").getAbsoluteFile().getPath(), "/file.txt");
      assertEquals(fs.getFile("file.txt").getAbsoluteFile().getPath(), "/file.txt");
      assertEquals(fs.getFile("dir/file.txt").getAbsoluteFile().getPath(), "/dir/file.txt");
   }

   public void testIsAbsolute() throws IOException {
      assertTrue(fs.getFile("/file.txt").isAbsolute());
      assertFalse(fs.getFile("file.txt").isAbsolute());
   }

   public void testLeadingSeparatorIsOptional() throws IOException {
      File gridFile = fs.getFile("file.txt");
      assert gridFile.createNewFile();

      assertTrue(fs.getFile("file.txt").exists());
      assertTrue(fs.getFile("/file.txt").exists());

      File dir = fs.getFile("dir");
      boolean dirCreated = dir.mkdir();
      assertTrue(dirCreated);

      assertTrue(fs.getFile("dir").exists());
      assertTrue(fs.getFile("/dir").exists());
   }

   public void testGetName() throws IOException {
      assertEquals(fs.getFile("").getName(), "");
      assertEquals(fs.getFile("/").getName(), "");
      assertEquals(fs.getFile("file.txt").getName(), "file.txt");
      assertEquals(fs.getFile("/file.txt").getName(), "file.txt");
      assertEquals(fs.getFile("/dir/file.txt").getName(), "file.txt");
      assertEquals(fs.getFile("/dir/subdir/file.txt").getName(), "file.txt");
      assertEquals(fs.getFile("dir/subdir/file.txt").getName(), "file.txt");
   }

   public void testEquals() throws Exception {
      assertFalse(fs.getFile("").equals(null));
      assertTrue(fs.getFile("").equals(fs.getFile("")));
      assertTrue(fs.getFile("").equals(fs.getFile("/")));
      assertTrue(fs.getFile("foo.txt").equals(fs.getFile("foo.txt")));
      assertTrue(fs.getFile("foo.txt").equals(fs.getFile("/foo.txt")));
      assertFalse(fs.getFile("foo.txt").equals(fs.getFile("FOO.TXT")));
      assertFalse(fs.getFile("/foo.txt").equals(new File("/foo.txt")));
   }

   private String getStringFromChannel(ReadableByteChannel channel, int length) throws IOException {
      ByteBuffer buffer = ByteBuffer.allocate(length);
      channel.read(buffer);
      return getStringFrom(buffer);
   }

   private String getStringFrom(ByteBuffer buffer) {
      buffer.flip();
      byte[] buf = new byte[buffer.remaining()];
      buffer.get(buf);
      return new String(buf);
   }

   private String[] getPaths(File[] files) {
      String[] paths = new String[files.length];
      for (int i = 0; i < files.length; i++) {
         File file = files[i];
         paths[i] = file.getPath();
      }
      return paths;
   }

   private Set<String> asSet(String... strings) {
      return new HashSet<String>(Arrays.asList(strings));
   }

   private File createDirWithFiles() throws IOException {
      File dir = createDir("/myDir");
      createFile("/myDir/foo1.txt");
      createFile("/myDir/foo2.txt");
      createFile("/myDir/bar1.txt");
      createFile("/myDir/bar2.txt");
      createDir("/myDir/fooDir");
      createFile("/myDir/fooDir/foo.txt");
      createFile("/myDir/fooDir/bar.txt");
      createDir("/myDir/barDir");
      return dir;
   }

   private File createFile(String pathname) throws IOException {
      File file = fs.getFile(pathname);
      assert file.createNewFile();
      return file;
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

   private static class FooFilenameFilter implements FilenameFilter {
      private File expectedDir;

      @Override
      public boolean accept(File dir, String name) {
         if (expectedDir != null)
            assertEquals(dir, expectedDir, "accept() invoked with unexpected dir");
         return name.startsWith("foo");
      }

      public void expectDir(File dir) {
         expectedDir = dir;
      }
   }

   private static class FooFileFilter implements FileFilter {
      @Override
      public boolean accept(File file) {
         return file.getName().startsWith("foo");
      }
   }
}
