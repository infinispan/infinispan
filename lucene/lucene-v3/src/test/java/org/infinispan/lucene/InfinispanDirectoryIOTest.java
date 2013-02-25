/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.lucene;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RAMDirectory;
import org.infinispan.Cache;
import org.infinispan.lucene.directory.DirectoryBuilder;
import org.infinispan.lucene.impl.DirectoryBuilderImpl;
import org.infinispan.lucene.impl.DirectoryExtensions;
import org.infinispan.lucene.testutils.RepeatableLongByteSequence;
import org.infinispan.manager.CacheContainer;
import org.infinispan.test.TestingUtil;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

/**
 * @author Lukasz Moren
 * @author Davide Di Somma
 * @author Sanne Grinovero
 */
@SuppressWarnings("unchecked")
@Test(groups = "functional", testName = "lucene.InfinispanDirectoryIOTest", sequential = true)
public class InfinispanDirectoryIOTest {
   
   /** The Test index name */
   private static final String INDEXNAME = "index";

   private CacheContainer cacheManager;
   private File indexDir = new File(new File("."), INDEXNAME);

   @BeforeTest
   public void prepareCacheManager() {
      cacheManager = CacheTestSupport.createTestCacheManager();
   }

   @AfterTest(alwaysRun=true)
   public void killCacheManager() {
      TestingUtil.killCacheManagers(cacheManager);
   }

   @AfterMethod(alwaysRun=true)
   public void clearCache() {
      cacheManager.getCache().clear();
      TestingUtil.recursiveFileRemove(indexDir);
   }

   @Test
   public void testWriteUsingSeekMethod() throws IOException {
      final int BUFFER_SIZE = 64;
      
      Cache cache = cacheManager.getCache();
      Directory dir = DirectoryBuilder.newDirectoryInstance(cache, cache, cache, INDEXNAME).chunkSize(BUFFER_SIZE).create();
      
      String fileName = "SomeText.txt";
      IndexOutput io = dir.createOutput(fileName);
      RepeatableLongByteSequence bytesGenerator = new RepeatableLongByteSequence();
      //It writes repeatable text
      final int REPEATABLE_BUFFER_SIZE = 1501;
      for (int i = 0; i < REPEATABLE_BUFFER_SIZE; i++) {
         io.writeByte(bytesGenerator.nextByte());
      }
      io.flush();
      assert io.length() == REPEATABLE_BUFFER_SIZE;
      
      //Text to write on file with repeatable text
      final String someText = "This is some text";
      final byte[] someTextAsBytes = someText.getBytes();
      //4 points in random order where writing someText: at begin of file, at end of file, within a single chunk,
      //between 2 chunks
      final int[] pointers = {0, 635, REPEATABLE_BUFFER_SIZE, 135};
      for(int i=0; i < pointers.length; i++) {
         io.seek(pointers[i]);
         io.writeBytes(someTextAsBytes, someTextAsBytes.length);
      }
      
      io.close();
      bytesGenerator.reset();
      final long finalSize = REPEATABLE_BUFFER_SIZE + someTextAsBytes.length;
      assert io.length() == finalSize;
      assert io.length() == DirectoryIntegrityCheck.deepCountFileSize(new FileCacheKey(INDEXNAME,fileName), cache);
      
      int indexPointer = 0;
      Arrays.sort(pointers);
      byte[] buffer = null;
      int chunkIndex = -1;
      //now testing the stream is equal to the produced repeatable but including the edits at pointed positions
      for (int i = 0; i < REPEATABLE_BUFFER_SIZE + someTextAsBytes.length; i++) {
         if (i % BUFFER_SIZE == 0) {
            buffer = (byte[]) cache.get(new ChunkCacheKey(INDEXNAME, fileName, ++chunkIndex, BUFFER_SIZE));
         }
         
         byte predictableByte = bytesGenerator.nextByte();
         if (i < pointers[indexPointer]) {
            //Assert predictable text
            Assert.assertEquals(predictableByte, buffer[i % BUFFER_SIZE]);
         } else if (pointers[indexPointer] <= i && i < pointers[indexPointer] + someTextAsBytes.length) {
            //Assert someText 
            Assert.assertEquals(someTextAsBytes[i - pointers[indexPointer]], buffer[i % BUFFER_SIZE]);
         }
         
         if (i == pointers[indexPointer] + someTextAsBytes.length) {
            //Change pointer
            indexPointer++;
         }
      }
         
      dir.close();
      DirectoryIntegrityCheck.verifyDirectoryStructure(cache, INDEXNAME);
   }

   @Test
   public void testReadWholeFile() throws IOException {
      final int BUFFER_SIZE = 64;

      Cache cache = cacheManager.getCache();
      Directory dir = DirectoryBuilder.newDirectoryInstance(cache, cache, cache, INDEXNAME).chunkSize(BUFFER_SIZE).create();

      verifyOnBuffer("SingleChunk.txt", 61, BUFFER_SIZE, cache, dir, 15);

      final int VERY_BIG_FILE_SIZE = 10000;
      assert BUFFER_SIZE < VERY_BIG_FILE_SIZE;
      verifyOnBuffer("MultipleChunks.txt", VERY_BIG_FILE_SIZE, BUFFER_SIZE, cache, dir, 33);

      final int LAST_CHUNK_COMPLETELY_FILLED_FILE_SIZE = 256;
      assert (LAST_CHUNK_COMPLETELY_FILLED_FILE_SIZE % BUFFER_SIZE) == 0;
      verifyOnBuffer("LastChunkFilled.txt", LAST_CHUNK_COMPLETELY_FILLED_FILE_SIZE, BUFFER_SIZE, cache, dir, 11);
      assertHasNChunks(4, cache, INDEXNAME, "LastChunkFilled.txt.bak", BUFFER_SIZE);
      DirectoryIntegrityCheck.verifyDirectoryStructure(cache, INDEXNAME);

      final int LAST_CHUNK_WITH_LONELY_BYTE_FILE_SIZE = 257;
      assert (LAST_CHUNK_WITH_LONELY_BYTE_FILE_SIZE % BUFFER_SIZE) == 1;
      verifyOnBuffer("LonelyByteInLastChunk.txt", LAST_CHUNK_WITH_LONELY_BYTE_FILE_SIZE, BUFFER_SIZE, cache, dir, 12);
      assertHasNChunks(5, cache, INDEXNAME, "LonelyByteInLastChunk.txt.bak", BUFFER_SIZE);
      
      dir.close();
      DirectoryIntegrityCheck.verifyDirectoryStructure(cache, INDEXNAME);
   }

   /**
    * Helper for testReadWholeFile test:
    * creates a file and then verifies it's readability in specific corner cases.
    * Then reuses the same parameters to verify the file rename capabilities. 
    */
   private void verifyOnBuffer(final String fileName, final int fileSize, final int bufferSize, Cache cache, Directory dir, final int readBuffer) throws IOException {
      createFileWithRepeatableContent(dir, fileName, fileSize);
      assertReadByteWorkingCorrectly(dir, fileName, fileSize);
      assertReadBytesWorkingCorrectly(dir, fileName, fileSize, readBuffer);
      DirectoryIntegrityCheck.verifyDirectoryStructure(cache, INDEXNAME);
      final String newFileName = fileName+".bak";
      ((DirectoryExtensions)dir).renameFile(fileName, newFileName);
      assertReadByteWorkingCorrectly(dir, newFileName, fileSize);
      assertReadBytesWorkingCorrectly(dir, newFileName, fileSize, readBuffer);
      DirectoryIntegrityCheck.verifyDirectoryStructure(cache, INDEXNAME);
      assert dir.fileExists(newFileName);
      assert dir.fileExists(fileName) == false;
   }
   
   @Test
   public void testReadRandomSampleFile() throws IOException {
      final int BUFFER_SIZE = 64;

      Cache cache = cacheManager.getCache();
      Directory dir = DirectoryBuilder.newDirectoryInstance(cache, cache, cache, INDEXNAME).chunkSize(BUFFER_SIZE).create();

      final int FILE_SIZE = 1000;
      assert BUFFER_SIZE < FILE_SIZE;
      createFileWithRepeatableContent(dir, "RandomSampleFile.txt", FILE_SIZE);

      IndexInput indexInput = dir.openInput("RandomSampleFile.txt");
      assert indexInput.length() == FILE_SIZE;
      RepeatableLongByteSequence bytesGenerator = new RepeatableLongByteSequence();

      Random r = new Random();
      long seekPoint = 0;
      // Now it reads some random byte and it compares to the expected byte
      for (int i = 0; i < FILE_SIZE; i++) {
         if (seekPoint == i) {
            byte expectedByte = bytesGenerator.nextByte();
            byte actualByte = indexInput.readByte();
            assert expectedByte == actualByte;
            seekPoint = indexInput.getFilePointer() + r.nextInt(10);
            indexInput.seek(seekPoint);
         } else {
            bytesGenerator.nextByte();
         }

      }
      indexInput.close();
      dir.close();
      DirectoryIntegrityCheck.verifyDirectoryStructure(cache, INDEXNAME);
   }
   
   /**
    * Used to verify that IndexInput.readBytes method reads correctly the whole file content comparing the
    * result with the expected sequence of bytes
    * 
    * @param dir
    *           The Directory containing the file to verify
    * @param fileName
    *           The file name to read
    * @param contentFileSizeExpected
    *           The size content file expected
    * @param arrayLengthToRead
    *           The length of byte array to read
    * @throws IOException
    */
   private void assertReadBytesWorkingCorrectly(Directory dir, String fileName,
            final int contentFileSizeExpected, final int arrayLengthToRead) throws IOException {
      IndexInput indexInput = dir.openInput(fileName);
      Assert.assertEquals(contentFileSizeExpected, indexInput.length());

      RepeatableLongByteSequence bytesGenerator = new RepeatableLongByteSequence();

      byte[] readBytes = new byte[arrayLengthToRead];
      byte[] expectedBytes = new byte[arrayLengthToRead];

      long toRead = contentFileSizeExpected;
      while (toRead > 0) {
         // the condition is satisfied when the file is close to the end
         if (toRead < arrayLengthToRead) {
            readBytes = new byte[(int) toRead];
            expectedBytes = new byte[(int) toRead];
         }
         int nextBytesToRead = (int) Math.min(toRead, arrayLengthToRead);

         bytesGenerator.nextBytes(expectedBytes);
         indexInput.readBytes(readBytes, 0, nextBytesToRead);

         assert Arrays.equals(expectedBytes, readBytes);

         toRead = toRead - nextBytesToRead;

      }
      indexInput.close();
   }

   /**
    * Used to verify that IndexInput.readByte method reads correctly the whole file content comparing the
    * result with the expected sequence of bytes
    * 
    * @param dir
    *           The Directory containing the file to verify
    * @param fileName
    *           The file name to read
    * @param contentFileSizeExpected
    *           The size content file expected
    * @throws IOException
    */
   private void assertReadByteWorkingCorrectly(Directory dir, String fileName,
            final int contentFileSizeExpected) throws IOException {
      IndexInput indexInput = dir.openInput(fileName);
      Assert.assertEquals(contentFileSizeExpected, indexInput.length());
      RepeatableLongByteSequence bytesGenerator = new RepeatableLongByteSequence();

      for (int i = 0; i < contentFileSizeExpected; i++) {
         Assert.assertEquals(bytesGenerator.nextByte(), indexInput.readByte());
      }
      indexInput.close();
   }

   /**
    * Verifies a file is divided in N chunks
    */
   private void assertHasNChunks(int expectedChunks, Cache cache, String index, String fileName, int bufferSize) {
      int i=0;
      for (; i<expectedChunks; i++) {
         ChunkCacheKey key = new ChunkCacheKey(index, fileName, i, bufferSize);
         Assert.assertTrue(cache.containsKey(key), "should contain key " + key);
      }
      ChunkCacheKey key = new ChunkCacheKey(index, fileName, i, bufferSize);
      Assert.assertFalse(cache.containsKey(key), "should NOT contain key " + key);
   }

   /**
    * It creates a file with fixed size using a RepeatableLongByteSequence object to generate a
    * repeatable content
    * 
    * @param dir
    *           The Directory containing the file to create
    * @param fileName
    *           The file name to create
    * @param contentFileSize
    *           The size content file to create
    * @throws IOException
    */
   private void createFileWithRepeatableContent(Directory dir, String fileName, final int contentFileSize) throws IOException {
      IndexOutput indexOutput = dir.createOutput(fileName);
      RepeatableLongByteSequence bytesGenerator = new RepeatableLongByteSequence();
      for (int i = 0; i < contentFileSize; i++) {
         indexOutput.writeByte(bytesGenerator.nextByte());
      }
      indexOutput.close();
   }

   
   @Test(enabled = false)
   public void testReadChunks() throws Exception {
      final int BUFFER_SIZE = 64;

      Cache cache = cacheManager.getCache();
      Directory dir = DirectoryBuilder.newDirectoryInstance(cache, cache, cache, INDEXNAME).chunkSize(BUFFER_SIZE).create();

      // create file headers
      FileMetadata file1 = new FileMetadata(5);
      FileCacheKey key1 = new FileCacheKey(INDEXNAME, "Hello.txt");
      cache.put(key1, file1);

      FileMetadata file2 = new FileMetadata(5);
      FileCacheKey key2 = new FileCacheKey(INDEXNAME, "World.txt");
      cache.put(key2, file2);

      // byte array for Hello.txt
      String helloText = "Hello world.  This is some text.";
      cache.put(new ChunkCacheKey(INDEXNAME, "Hello.txt", 0, BUFFER_SIZE), helloText.getBytes());

      // byte array for World.txt - should be in at least 2 chunks.
      String worldText = "This String should contain more than sixty four characters but less than one hundred and twenty eight.";
      assert worldText.getBytes().length > BUFFER_SIZE;
      assert worldText.getBytes().length < (2 * BUFFER_SIZE);

      byte[] buf = new byte[BUFFER_SIZE];
      System.arraycopy(worldText.getBytes(), 0, buf, 0, BUFFER_SIZE);
      cache.put(new ChunkCacheKey(INDEXNAME, "World.txt", 0, BUFFER_SIZE), buf);

      String part1 = new String(buf);
      buf = new byte[BUFFER_SIZE];
      System.arraycopy(worldText.getBytes(), BUFFER_SIZE, buf, 0, worldText.length() - BUFFER_SIZE);
      cache.put(new ChunkCacheKey(INDEXNAME, "World.txt", 1, BUFFER_SIZE), buf);
      String part2 = new String(buf);

      // make sure the generated bytes do add up!
      Assert.assertEquals(part1 + part2.trim(), worldText);

      file1.setSize(helloText.length());
      file2.setSize(worldText.length());

      Set<String> s = new HashSet<String>();
      s.add("Hello.txt");
      s.add("World.txt");
      Set other = new HashSet(Arrays.asList(dir.listAll()));

      // ok, file listing works.
      Assert.assertEquals(s, other);

      IndexInput ii = dir.openInput("Hello.txt");

      assert ii.length() == helloText.length();

      ByteArrayOutputStream baos = new ByteArrayOutputStream();

      for (int i = 0; i < ii.length(); i++) {
         baos.write(ii.readByte());
      }

      assert new String(baos.toByteArray()).equals(helloText);

      ii = dir.openInput("World.txt");

      assert ii.length() == worldText.length();

      baos = new ByteArrayOutputStream();

      for (int i = 0; i < ii.length(); i++) {
         baos.write(ii.readByte());
      }

      assert new String(baos.toByteArray()).equals(worldText);

      // now with buffered reading

      ii = dir.openInput("Hello.txt");

      assert ii.length() == helloText.length();

      baos = new ByteArrayOutputStream();

      long toRead = ii.length();
      while (toRead > 0) {
         buf = new byte[19]; // suitably arbitrary
         int bytesRead = (int) Math.min(toRead, 19);
         ii.readBytes(buf, 0, bytesRead);
         toRead = toRead - bytesRead;
         baos.write(buf, 0, bytesRead);
      }

      assert new String(baos.toByteArray()).equals(helloText);

      ii = dir.openInput("World.txt");

      assert ii.length() == worldText.length();

      baos = new ByteArrayOutputStream();

      toRead = ii.length();
      while (toRead > 0) {
         buf = new byte[19]; // suitably arbitrary
         int bytesRead = (int) Math.min(toRead, 19);
         ii.readBytes(buf, 0, bytesRead);
         toRead = toRead - bytesRead;
         baos.write(buf, 0, bytesRead);
      }

      assert new String(baos.toByteArray()).equals(worldText);

      dir.deleteFile("Hello.txt");
      assert null == cache.get(new FileCacheKey(INDEXNAME, "Hello.txt"));
      assert null == cache.get(new ChunkCacheKey(INDEXNAME, "Hello.txt", 0, BUFFER_SIZE));

      Object ob1 = cache.get(new FileCacheKey(INDEXNAME, "World.txt"));
      Object ob2 = cache.get(new ChunkCacheKey(INDEXNAME, "World.txt", 0, BUFFER_SIZE));
      Object ob3 = cache.get(new ChunkCacheKey(INDEXNAME, "World.txt", 1, BUFFER_SIZE));

      ((DirectoryExtensions)dir).renameFile("World.txt", "HelloWorld.txt");
      assert null == cache.get(new FileCacheKey(INDEXNAME, "Hello.txt"));
      assert null == cache.get(new ChunkCacheKey(INDEXNAME, "Hello.txt", 0, BUFFER_SIZE));
      assert null == cache.get(new ChunkCacheKey(INDEXNAME, "Hello.txt", 1, BUFFER_SIZE));

      assert cache.get(new FileCacheKey(INDEXNAME, "HelloWorld.txt")).equals(ob1);
      assert cache.get(new ChunkCacheKey(INDEXNAME, "HelloWorld.txt", 0, BUFFER_SIZE)).equals(ob2);
      assert cache.get(new ChunkCacheKey(INDEXNAME, "HelloWorld.txt", 1, BUFFER_SIZE)).equals(ob3);

      // test that contents survives a move
      ii = dir.openInput("HelloWorld.txt");

      assert ii.length() == worldText.length();

      baos = new ByteArrayOutputStream();

      toRead = ii.length();
      while (toRead > 0) {
         buf = new byte[19]; // suitably arbitrary
         int bytesRead = (int) Math.min(toRead, 19);
         ii.readBytes(buf, 0, bytesRead);
         toRead = toRead - bytesRead;
         baos.write(buf, 0, bytesRead);
      }

      assert new String(baos.toByteArray()).equals(worldText);

      dir.close();
      DirectoryIntegrityCheck.verifyDirectoryStructure(cache, INDEXNAME);
   }

   public void testWriteChunks() throws Exception {
      final int BUFFER_SIZE = 64;

      Cache cache = cacheManager.getCache();
      Directory dir = DirectoryBuilder.newDirectoryInstance(cache, cache, cache, INDEXNAME).chunkSize(BUFFER_SIZE).create();

      IndexOutput io = dir.createOutput("MyNewFile.txt");

      io.writeByte((byte) 66);
      io.writeByte((byte) 69);

      io.flush();
      io.close();

      assert dir.fileExists("MyNewFile.txt");
      assert null != cache.get(new ChunkCacheKey(INDEXNAME, "MyNewFile.txt", 0, BUFFER_SIZE));

      // test contents by reading:
      byte[] buf = new byte[9];
      IndexInput ii = dir.openInput("MyNewFile.txt");
      ii.readBytes(buf, 0, (int) ii.length());
      ii.close();

      assert new String(new byte[] { 66, 69 }).equals(new String(buf).trim());

      String testText = "This is some rubbish again that will span more than one chunk - one hopes.  Who knows, maybe even three or four chunks.";
      io = dir.createOutput("MyNewFile.txt");
      io.seek(0);
      io.writeBytes(testText.getBytes(), 0, testText.length());
      io.close();
      // now compare.
      byte[] chunk1 = (byte[]) cache.get(new ChunkCacheKey(INDEXNAME, "MyNewFile.txt", 0, BUFFER_SIZE));
      byte[] chunk2 = (byte[]) cache.get(new ChunkCacheKey(INDEXNAME, "MyNewFile.txt", 1, BUFFER_SIZE));
      assert null != chunk1;
      assert null != chunk2;

      assert testText.equals(new String(chunk1) + new String(chunk2).trim());

      dir.close();
      DirectoryIntegrityCheck.verifyDirectoryStructure(cache, INDEXNAME);
   }

   @Test
   public void testWriteChunksDefaultChunks() throws Exception {
      Cache cache = cacheManager.getCache();
      Directory dir = DirectoryBuilder.newDirectoryInstance(cache, cache, cache, INDEXNAME).create();

      final String testText = "This is some rubbish";
      final byte[] testTextAsBytes = testText.getBytes();

      IndexOutput io = dir.createOutput("MyNewFile.txt");

      io.writeByte((byte) 1);
      io.writeByte((byte) 2);
      io.writeByte((byte) 3);
      io.writeBytes(testTextAsBytes, testTextAsBytes.length);
      io.close();
      DirectoryIntegrityCheck.verifyDirectoryStructure(cache, INDEXNAME);

      FileCacheKey fileCacheKey = new FileCacheKey(INDEXNAME, "MyNewFile.txt");
      assert null != cache.get(fileCacheKey);
      FileMetadata metadata = (FileMetadata) cache.get(fileCacheKey);
      Assert.assertEquals(testTextAsBytes.length + 3, metadata.getSize());
      assert null != cache.get(new ChunkCacheKey(INDEXNAME, "MyNewFile.txt", 0, DirectoryBuilderImpl.DEFAULT_BUFFER_SIZE));

      // test contents by reading:
      IndexInput ii = dir.openInput("MyNewFile.txt");
      assert ii.readByte() == 1;
      assert ii.readByte() == 2;
      assert ii.readByte() == 3;
      byte[] buf = new byte[testTextAsBytes.length];

      ii.readBytes(buf, 0, testTextAsBytes.length);
      ii.close();

      assert testText.equals(new String(buf).trim());

      dir.close();
      DirectoryIntegrityCheck.verifyDirectoryStructure(cache, INDEXNAME);
   }
   
   @Test
   public void testChunkBordersOnInfinispan() throws IOException {
      Cache cache = cacheManager.getCache();
      cache.clear();
      Directory dir = DirectoryBuilder.newDirectoryInstance(cache, cache, cache, INDEXNAME).chunkSize(13).create();
      testChunkBorders(dir, cache);
      cache.clear();
   }
   
   @Test
   public void testChunkBordersOnRAMDirectory() throws IOException {
      RAMDirectory dir = new RAMDirectory();
      testChunkBorders(dir, null);
   }
   
   /**
    * Useful to verify the Infinispan Directory has similar behaviour
    * to standard Lucene implementations regarding reads out of ranges.
    */
   private void testChunkBorders(Directory dir, Cache cache) throws IOException {
      //numbers are chosen to be multiples of the chunksize set for the InfinispanDirectory
      //so that we test the borders of it.
      
      testOn(dir, 0 ,0, cache);
      testOn(dir, 0 ,1, cache);
      testOn(dir, 1 ,1, cache);
      testOn(dir, 1 ,0, cache);
      
      // all equal:
      testOn(dir, 13 ,13, cache);
      
      // one less:
      testOn(dir, 12 ,13, cache);
      testOn(dir, 13 ,12, cache);
      testOn(dir, 12 ,12, cache);
      
      // one higher
      testOn(dir, 13 ,14, cache);
      testOn(dir, 14 ,13, cache);
      testOn(dir, 14 ,14, cache);
      
      // now repeat in multi-chunk scenario:
      // all equal:
      testOn(dir, 39 ,39, cache);
      
      // one less:
      testOn(dir, 38 ,38, cache);
      testOn(dir, 38 ,39, cache);
      testOn(dir, 39 ,38, cache);
      
      // one higher
      testOn(dir, 40 ,40, cache);
      testOn(dir, 40 ,39, cache);
      testOn(dir, 39 ,40, cache);
      
      // quite bigger
      testOn(dir, 600, 600, cache);
   }

   private void testOn(Directory dir, int writeSize, int readSize, Cache cache) throws IOException {
      if (cache != null) cache.clear();//needed to make sure no chunks are left over in case of Infinispan implementation
      final String filename = "chunkTest";
      IndexOutput indexOutput = dir.createOutput(filename);
      byte[] toWrite = fillBytes(writeSize);
      indexOutput.writeBytes(toWrite, writeSize);
      indexOutput.close();
      if (cache != null) {
         Assert.assertEquals(writeSize, DirectoryIntegrityCheck.deepCountFileSize(new FileCacheKey(INDEXNAME,filename), cache));
      }
      Assert.assertEquals(writeSize, indexOutput.length());
      byte[] results = new byte[readSize];
      IndexInput openInput = dir.openInput(filename);
      try {
         openInput.readBytes(results, 0, readSize);
         for (int i = 0; i < writeSize && i < readSize; i++) {
            Assert.assertEquals(results[i], toWrite[i]);
         }
         if (readSize > writeSize)
            Assert.fail("should have thrown an IOException for reading past EOF");
      } catch (IOException ioe) {
         if (readSize <= writeSize)
            Assert.fail("should not have thrown an IOException" + ioe.getMessage());
      }
   }
   
   public void multipleFlushTest() throws IOException {
      final String filename = "longFile.writtenInMultipleFlushes";
      final int bufferSize = 300;
      Cache cache = cacheManager.getCache();
      cache.clear();
      Directory dir = DirectoryBuilder.newDirectoryInstance(cache, cache, cache, INDEXNAME).chunkSize(13).create();
      byte[] manyBytes = fillBytes(bufferSize);
      IndexOutput indexOutput = dir.createOutput(filename);
      for (int i = 0; i < 10; i++) {
         indexOutput.writeBytes(manyBytes, bufferSize);
         indexOutput.flush();
      }
      indexOutput.close();
      IndexInput input = dir.openInput(filename);
      final int finalSize = (10 * bufferSize);
      Assert.assertEquals(finalSize, input.length());
      final byte[] resultingBuffer = new byte[finalSize];
      input.readBytes(resultingBuffer, 0, finalSize);
      int index = 0;
      for (int i = 0; i < 10; i++) {
         for (int j = 0; j < bufferSize; j++)
            Assert.assertEquals(resultingBuffer[index++], manyBytes[j]);
      }
   }

   private byte[] fillBytes(int size) {
      byte[] b = new byte[size];
      for (int i=0; i<size; i++) {
         b[i]=(byte)i;
      }
      return b;
   }

}
