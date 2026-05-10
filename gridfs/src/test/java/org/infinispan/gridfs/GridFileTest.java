package org.infinispan.gridfs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileFilter;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(testName = "io.GridFileTest", groups = "functional")
public class GridFileTest extends SingleCacheManagerTest {

   private Cache<String, byte[]> dataCache;
   private Cache<String, GridFile.Metadata> metadataCache;
   private GridFilesystem fs;

   @Override
   protected EmbeddedCacheManager createCacheManager() {
      EmbeddedCacheManager cacheManager = TestCacheManagerFactory.createCacheManager();
      Configuration configuration = new ConfigurationBuilder().build();
      cacheManager.defineConfiguration("data", configuration);
      cacheManager.defineConfiguration("metadata", configuration);
      return cacheManager;
   }

   @BeforeMethod
   protected void setUp() {
      dataCache = cacheManager.getCache("data");
      metadataCache = cacheManager.getCache("metadata");
      fs = new GridFilesystem(dataCache, metadataCache);
   }

   public void testGridFS() throws IOException {
      File gridDir = fs.getFile("/test");
      assertTrue(gridDir.mkdirs());
      File gridFile = fs.getFile("/test/myfile.txt");
      assertTrue(gridFile.createNewFile());
   }

   public void testGetFile() {
      assertEquals("file.txt", fs.getFile("file.txt").getPath());
      assertEquals("/file.txt", fs.getFile("/file.txt").getPath());
      assertEquals("myDir/file.txt", fs.getFile("myDir/file.txt").getPath());
      assertEquals("/myDir/file.txt", fs.getFile("/myDir/file.txt").getPath());

      assertEquals("myDir/file.txt", fs.getFile("myDir", "file.txt").getPath());
      assertEquals("/myDir/file.txt", fs.getFile("/myDir", "file.txt").getPath());

      File dir = fs.getFile("/myDir");
      assertEquals("/myDir/file.txt", fs.getFile(dir, "file.txt").getPath());

      dir = fs.getFile("myDir");
      assertEquals("myDir/file.txt", fs.getFile(dir, "file.txt").getPath());
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

   public void testNonExistentFileIsNeitherFileNorDirectory() {
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

   public void testGetParent() {
      File file = fs.getFile("file.txt");
      assertNull(file.getParent());

      file = fs.getFile("/parentdir/file.txt");
      assertEquals("/parentdir", file.getParent());

      file = fs.getFile("/parentdir/subdir/file.txt");
      assertEquals("/parentdir/subdir", file.getParent());
   }

   public void testGetParentFile() {
      File file = fs.getFile("file.txt");
      assertNull(file.getParentFile());

      file = fs.getFile("/parentdir/file.txt");
      File parentDir = file.getParentFile();
      assertInstanceOf(GridFile.class, parentDir);
      assertEquals("/parentdir", parentDir.getPath());
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
      assertTrue(created);
      return dir;
   }

   public void testWriteAcrossMultipleChunksWithNonDefaultChunkSize() throws Exception {
      writeToFile("multipleChunks.txt",
                  "This text spans multiple chunks, because each chunk is only 10 bytes long.",
                  10);  // chunkSize = 10

      String text = getContents("multipleChunks.txt");
      assertEquals("This text spans multiple chunks, because each chunk is only 10 bytes long.", text);
   }

   public void testWriteAcrossMultipleChunksWithNonDefaultChunkSizeAfterFileIsExplicitlyCreated() throws Exception {
      GridFile file = (GridFile) fs.getFile("multipleChunks.txt", 20);  // chunkSize = 20
      file.createNewFile();

      writeToFile("multipleChunks.txt",
                  "This text spans multiple chunks, because each chunk is only 20 bytes long.",
                  10);  // chunkSize = 10 (but it is ignored, because the file was already created with chunkSize = 20

      String text = getContents("multipleChunks.txt");
      assertEquals("This text spans multiple chunks, because each chunk is only 20 bytes long.", text);
   }

   public void testAppendToFileThatEndsWithFullChunk() throws Exception {
      writeToFile("endsWithFullChunk.txt", "1234" + "5678", 4);  // chunkSize = 4; two chunks will be written; both chunks will be full
      appendToFile("endsWithFullChunk.txt", "", 4);
      assertEquals("12345678", getContents("endsWithFullChunk.txt"));
   }

   public void testAppend() throws Exception {
      writeToFile("append.txt", "Hello");
      appendToFile("append.txt", "World");
      assertEquals("HelloWorld", getContents("append.txt"));
   }

   public void testAppendWithDifferentChunkSize() throws Exception {
      writeToFile("append.txt", "Hello", 2);   // chunkSize = 2
      appendToFile("append.txt", "World", 5);        // chunkSize = 5
      assertEquals("HelloWorld", getContents("append.txt"));
   }

   public void testAppendToEmptyFile() throws Exception {
      appendToFile("empty.txt", "Hello");
      assertEquals("Hello", getContents("empty.txt"));
   }

   public void testDeleteRemovesAllChunks() throws Exception {
      assertEquals(0, numberOfChunksInCache());
      assertEquals(0, numberOfMetadataEntries());

      writeToFile("delete.txt", "delete me", 100);

      GridFile file = (GridFile) fs.getFile("delete.txt");
      boolean deleted = file.delete();
      assertTrue(deleted);
      assertFalse(file.exists());
      assertEquals(0, numberOfChunksInCache());
      assertEquals(0, numberOfMetadataEntries());
   }

   @Test(expectedExceptions = UnsupportedOperationException.class)
   public void testDeleteOnExit() {
      fs.getFile("nonsuch.txt").deleteOnExit();
   }

   public void testOverwritingFileDoesNotLeaveExcessChunksInCache() throws Exception {
      assertEquals(0, numberOfChunksInCache());

      writeToFile("leak.txt", "12345abcde12345", 5); // file length = 15, chunkSize = 5.  Chunk size should "upgrade" to 8
      assertEquals(2, numberOfChunksInCache());

      writeToFile("leak.txt", "12345678", 5);           // file length = 5, chunkSize = 5.  Chunk size should "upgrade" to 8
      assertEquals(1, numberOfChunksInCache());
   }

    //ISPN-2157
    public void testWriteAndReadNegativeByte() throws Exception {
        String filePath = "negative.dat";
        OutputStream out = fs.getOutput(filePath);
        try{
            out.write(-1);
        }finally{
            out.close();
        }
        InputStream in = fs.getInput(filePath);
        try{
            assertEquals(255, in.read());
        }finally{
            in.close();
        }
    }

    public void testWriteAfterClose() throws Exception {
        String filePath = "test_write_to_closed.dat";
        OutputStream out = fs.getOutput(filePath);

        try{
            out.write(1);
        }finally{
            out.close();
        }
        IOException e = null;
        try{
            out.write(2);
        }catch (IOException ex){
            e = ex;
        }
        assertNotNull(e);
        File f = fs.getFile(filePath);
        assertEquals(1, f.length());
    }

    public void testMultiClose() throws Exception {
        String filePath = "test_close.dat";
        OutputStream out = fs.getOutput(filePath);
        try{
            out.write(1);
        }finally{
            out.close();
            out.close();
        }
        File f = fs.getFile(filePath);
        assertEquals(1, f.length());
    }

    public void testCanReadClosed() throws Exception {
        String filePath = "file_read_closed.txt";
        OutputStream out = fs.getOutput(filePath);
        try{
            out.write(1);
            out.write(2);
            out.write(3);
        }finally{
            out.close();
        }
        InputStream in = fs.getInput(filePath);
        in.read();
        in.close();
        IOException e = null;
        try{
            in.read();
        }catch(IOException ex){
            e = ex;
        }
        assertNotNull(e);
    }

   public void testSkip() throws Exception {
      String filePath = "skip.txt";
      writeToFile(filePath, "abcde" + "fghij" + "klmno" + "pqrst" + "uvwxy" + "z", 5);

      InputStream in = fs.getInput(filePath);
      try {
         long skipped = in.skip(2); // skip inside current chunk
         assertEquals(2, skipped);
         assertEquals('c', (char)in.read());

         skipped = in.skip(2);  // skip to end of chunk
         assertEquals(2, skipped);
         assertEquals('f', (char)in.read());

         skipped = in.skip(6); // skip into next chunk
         assertEquals(6, skipped);
         assertEquals('m', (char)in.read());

         skipped = in.skip(9);      // skip _over_ next chunk
         assertEquals(9, skipped);
         assertEquals('w', (char)in.read());

         skipped = in.skip(-1);  // negative skip
         assertEquals(0, skipped);
         assertEquals('x', (char)in.read());

         skipped = in.skip(10);  // skip beyond EOF
         assertEquals(2, skipped);
         assertEquals(-1, in.read());
      } finally {
         in.close();
      }
   }

   @SuppressWarnings("ResultOfMethodCallIgnored")
   public void testAvailable() throws Exception {
      String filePath = "available.txt";
      writeToFile(filePath, "abcde" + "fghij" + "klmno" + "pqrst" + "uvwxy" + "z", 5); // Chunk size should get "upgraded" to 8

      InputStream in = fs.getInput(filePath);
      try {
         assertEquals(0, in.available()); // since first chunk hasn't been fetched yet
         in.read();
         assertEquals(7, in.available());
         in.skip(3);
         assertEquals(4, in.available());
         in.read();
         assertEquals(3, in.available());
         in.read();
         assertEquals(2, in.available());
      } finally {
         in.close();
      }
   }

   public void testLastModified() throws Exception {
      assertEquals(0, fs.getFile("nonExistentFile.txt").lastModified());

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

   public void testSetLastModified() throws IOException {
      assertFalse(fs.getFile("nonsuch").setLastModified(23));
      File file = createFile("file.txt");
      assertTrue(file.setLastModified(42));
      assertEquals(42, fs.getFile("file.txt").lastModified());
   }

   public void testList() throws Exception {
      assertNull(fs.getFile("nonExistentDir").list());
      assertEquals(0, createDir("/emptyDir").list().length);

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
      assertEquals(0, createDir("/emptyDir").listFiles().length);

      File dir = createDirWithFiles();
      File[] files = dir.listFiles();
      assertEquals(
            asSet(getPaths(files)),
            asSet("/myDir/foo1.txt", "/myDir/foo2.txt", "/myDir/fooDir",
                  "/myDir/bar1.txt", "/myDir/bar2.txt", "/myDir/barDir"));
   }

   public void testListFilesWhereNonChildPathStartsWithParent() throws Exception {
      File parentDir = createDir("/parentDir");
      assertEquals(0, parentDir.listFiles().length);
      assertEquals(0, createDir("/parentDir-NOT-CHILD").listFiles().length);
      assertEquals(0, parentDir.listFiles().length);
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
      assertEquals(1, filenames.length);
      assertEquals("foo.txt", filenames[0]);
   }

   public void testReadableChannel() throws Exception {
      String content = "This is the content of channelTest.txt";
      writeToFile("/channelTest.txt", content, 10);

      ReadableGridFileChannel channel = fs.getReadableChannel("/channelTest.txt");
      try {
         assertTrue(channel.isOpen());
         ByteBuffer buffer = ByteBuffer.allocate(1000);
         channel.read(buffer);
         assertEquals(content, getStringFrom(buffer));
      } finally {
         channel.close();
      }

      assertFalse(channel.isOpen());
   }

   public void testReadableChannelPosition() throws Exception {
      writeToFile("/position.txt", "0123456789", 3);

      ReadableGridFileChannel channel = fs.getReadableChannel("/position.txt");
      try {
         assertEquals(0, channel.position());

         channel.position(5);
         assertEquals(5, channel.position());
         assertEquals("567", getStringFromChannel(channel, 3));
         assertEquals(8, channel.position());

         channel.position(2);
         assertEquals(2, channel.position());
         assertEquals("23456", getStringFromChannel(channel, 5));
         assertEquals(7, channel.position());
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
      assertEquals("This file spans multiple chunks.", getContents("/channelTest.txt"));
   }

   public void testWritableChannelAppend() throws Exception {
      writeToFile("/append.txt", "Initial text.", 3);

      WritableGridFileChannel channel = fs.getWritableChannel("/append.txt", true);
      try {
         channel.write(ByteBuffer.wrap("Appended text.".getBytes()));
      } finally {
         channel.close();
      }
      assertEquals("Initial text.Appended text.", getContents("/append.txt"));
   }

   public void testReadLoop() throws Exception {
      try (WritableGridFileChannel wgfc = fs.getWritableChannel("/readTest.txt", false, 100)) {
         assertTrue(wgfc.isOpen());
         wgfc.write(ByteBuffer.wrap("This tests read loop.".getBytes()));
      }
      try (ReadableGridFileChannel rgfc = fs.getReadableChannel("/readTest.txt")) {
         assertEquals("This tests read loop.", new String(toBytes(Channels.newInputStream(rgfc))));
      }
   }

   public void testGetAbsolutePath() throws IOException {
      assertEquals("/file.txt", fs.getFile("/file.txt").getAbsolutePath());
      assertEquals("/file.txt", fs.getFile("file.txt").getAbsolutePath());
      assertEquals("/dir/file.txt", fs.getFile("dir/file.txt").getAbsolutePath());
   }

   public void testGetAbsoluteFile() throws IOException {
      assertInstanceOf(GridFile.class, fs.getFile("file.txt").getAbsoluteFile());
      assertEquals("/file.txt", fs.getFile("/file.txt").getAbsoluteFile().getPath());
      assertEquals("/file.txt", fs.getFile("file.txt").getAbsoluteFile().getPath());
      assertEquals("/dir/file.txt", fs.getFile("dir/file.txt").getAbsoluteFile().getPath());
   }

   public void testIsAbsolute() throws IOException {
      assertTrue(fs.getFile("/file.txt").isAbsolute());
      assertFalse(fs.getFile("file.txt").isAbsolute());
   }

   @Test(expectedExceptions = UnsupportedOperationException.class)
   public void testRenameTo(){
      fs.getFile("file.txt").renameTo(null);
   }

   public void testLeadingSeparatorIsOptional() throws IOException {
      File gridFile = fs.getFile("file.txt");
      assertTrue(gridFile.createNewFile());

      assertTrue(fs.getFile("file.txt").exists());
      assertTrue(fs.getFile("/file.txt").exists());

      File dir = fs.getFile("dir");
      boolean dirCreated = dir.mkdir();
      assertTrue(dirCreated);

      assertTrue(fs.getFile("dir").exists());
      assertTrue(fs.getFile("/dir").exists());
   }

   public void testGetName() throws IOException {
      assertEquals("", fs.getFile("").getName());
      assertEquals("", fs.getFile("/").getName());
      assertEquals("file.txt", fs.getFile("file.txt").getName());
      assertEquals("file.txt", fs.getFile("/file.txt").getName());
      assertEquals("file.txt", fs.getFile("/dir/file.txt").getName());
      assertEquals("file.txt", fs.getFile("/dir/subdir/file.txt").getName());
      assertEquals("file.txt", fs.getFile("dir/subdir/file.txt").getName());
   }

   public void testEquals() throws Exception {
      assertNotEquals(null, fs.getFile(""));
      assertEquals(fs.getFile(""), fs.getFile(""));
      assertEquals(fs.getFile(""), fs.getFile("/"));
      assertEquals(fs.getFile("foo.txt"), fs.getFile("foo.txt"));
      assertEquals(fs.getFile("foo.txt"), fs.getFile("/foo.txt"));
      assertNotEquals(fs.getFile("foo.txt"), fs.getFile("FOO.TXT"));
      assertNotEquals(fs.getFile("/foo.txt"), new File("/foo.txt"));
   }

   public void testCanRead() throws Exception {
      File gridFile = fs.getFile("file.txt");
      assertTrue(gridFile.createNewFile());
      assertTrue(gridFile.canRead());
      assertFalse(fs.getFile("nonsuch.txt").canRead());
   }

   public void testCanWrite() throws Exception {
      File gridFile = fs.getFile("file.txt");
      assertTrue(gridFile.createNewFile());
      assertTrue(gridFile.canWrite());
      assertFalse(fs.getFile("nonsuch.txt").canWrite());
   }

   public void testIsHidden(){
      assertFalse(fs.getFile("nonsuch.txt").isHidden());
   }

   public void testCanExecute(){
      assertFalse(fs.getFile("nonsuch.txt").isHidden());
   }

   @Test(expectedExceptions = UnsupportedOperationException.class)
   public void testGetCanonicalPath() throws IOException {
      fs.getFile("nonsuch.txt").getCanonicalPath();
   }

   @Test(expectedExceptions = UnsupportedOperationException.class)
   public void testGetCanonicalFile() throws IOException {
      fs.getFile("nonsuch.txt").getCanonicalFile();
   }

   @Test(expectedExceptions = UnsupportedOperationException.class)
   public void testToURL() throws MalformedURLException {
      fs.getFile("nonsuch.txt").toURL();
   }

   @Test(expectedExceptions = UnsupportedOperationException.class)
   public void testToURI() {
      fs.getFile("nonsuch.txt").toURI();
   }

   @Test(expectedExceptions = UnsupportedOperationException.class)
   public void testSetReadOnly() {
      fs.getFile("nonsuch.txt").setReadOnly();
   }

   @Test(expectedExceptions = UnsupportedOperationException.class)
   public void testSetWritable() {
      fs.getFile("nonsuch.txt").setWritable(true);
   }

   @Test(expectedExceptions = UnsupportedOperationException.class)
   public void testSetWritable2() {
      fs.getFile("nonsuch.txt").setWritable(true, true);
   }

   @Test(expectedExceptions = UnsupportedOperationException.class)
   public void testSetReadable() {
      fs.getFile("nonsuch.txt").setReadable(true);
   }

   @Test(expectedExceptions = UnsupportedOperationException.class)
   public void testSetReadable2() {
      fs.getFile("nonsuch.txt").setReadable(true, true);
   }

   @Test(expectedExceptions = UnsupportedOperationException.class)
   public void testSetExecutable() {
      fs.getFile("nonsuch.txt").setExecutable(true);
   }

   @Test(expectedExceptions = UnsupportedOperationException.class)
   public void testSetExecutable2() {
      fs.getFile("nonsuch.txt").setExecutable(true,  true);
   }

   @Test(expectedExceptions = UnsupportedOperationException.class)
   public void testGetTotalSpace() {
      fs.getFile("nonsuch.txt").getTotalSpace();
   }

   @Test(expectedExceptions = UnsupportedOperationException.class)
   public void testGetFreeSpace() {
      fs.getFile("nonsuch.txt").getFreeSpace();
   }

   @Test(expectedExceptions = UnsupportedOperationException.class)
   public void testGetUsableSpace() {
      fs.getFile("nonsuch.txt").getUsableSpace();
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
      assertTrue(file.createNewFile());
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
      try (OutputStream out = chunkSize == null
            ? fs.getOutput(filePath, append)
            : fs.getOutput(filePath, append, chunkSize)) {
         out.write(text.getBytes());
      }
   }

   private String getContents(String filePath) throws IOException {
      InputStream in = fs.getInput(filePath);
      return getString(in);
   }

   private String getString(InputStream in) throws IOException {
      try (in) {
         byte[] buf = new byte[1000];
         int bytesRead = in.read(buf);
         return new String(buf, 0, bytesRead);
      }
   }

   private static byte[] toBytes(InputStream is) throws IOException {
      ByteArrayOutputStream buffer = new ByteArrayOutputStream();
      int nRead;
      byte[] data = new byte[16384];

      while ((nRead = is.read(data, 0, data.length)) != -1) {
         buffer.write(data, 0, nRead);
      }

      buffer.flush();
      return buffer.toByteArray();
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
