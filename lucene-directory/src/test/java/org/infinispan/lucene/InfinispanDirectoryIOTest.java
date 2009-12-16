/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2009, Red Hat Middleware LLC, and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.infinispan.Cache;
import org.infinispan.lucene.testutils.RepeatableLongByteSequence;
import org.testng.annotations.Test;

/**
 * @author Lukasz Moren
 * @author Davide Di Somma
 * @author Sanne Grinovero
 */
@Test(groups = "functional", testName = "lucene.InfinispanDirectoryIOTest")
public class InfinispanDirectoryIOTest {

   @Test
   public void testReadWholeFile() throws IOException {
      final int BUFFER_SIZE = 64;

      Cache<CacheKey, Object> cache = CacheTestSupport.createTestCacheManager().getCache();
      InfinispanDirectory dir = new InfinispanDirectory(cache, "index", BUFFER_SIZE);

      try {
         final int SHORT_FILE_SIZE = 61;
         assert BUFFER_SIZE > SHORT_FILE_SIZE;
         createFileWithRepeatableContent(dir, "SingleChunk.txt", SHORT_FILE_SIZE);
         assertReadByteWorkingCorrectly(dir, "SingleChunk.txt", SHORT_FILE_SIZE);
         assertReadBytesWorkingCorrectly(dir, "SingleChunk.txt", SHORT_FILE_SIZE, 15);

         final int VERY_BIG_FILE_SIZE = 10000;
         assert BUFFER_SIZE < VERY_BIG_FILE_SIZE;
         createFileWithRepeatableContent(dir, "MultipleChunks.txt", VERY_BIG_FILE_SIZE);
         assertReadByteWorkingCorrectly(dir, "MultipleChunks.txt", VERY_BIG_FILE_SIZE);
         assertReadBytesWorkingCorrectly(dir, "MultipleChunks.txt", VERY_BIG_FILE_SIZE, 33);

         final int LAST_CHUNK_COMPLETELY_FILLED_FILE_SIZE = 256;
         assert (LAST_CHUNK_COMPLETELY_FILLED_FILE_SIZE % BUFFER_SIZE) == 0;
         createFileWithRepeatableContent(dir, "LastChunkFilled.txt",
                  LAST_CHUNK_COMPLETELY_FILLED_FILE_SIZE);
         assertReadByteWorkingCorrectly(dir, "LastChunkFilled.txt",
                  LAST_CHUNK_COMPLETELY_FILLED_FILE_SIZE);
         assertReadBytesWorkingCorrectly(dir, "LastChunkFilled.txt",
                  LAST_CHUNK_COMPLETELY_FILLED_FILE_SIZE, 11);
         assert 4 == getChunksNumber(cache, "index", "LastChunkFilled.txt");

         final int LAST_CHUNK_WITH_LONELY_BYTE_FILE_SIZE = 257;
         assert (LAST_CHUNK_WITH_LONELY_BYTE_FILE_SIZE % BUFFER_SIZE) == 1;
         createFileWithRepeatableContent(dir, "LonelyByteInLastChunk.txt",
                  LAST_CHUNK_WITH_LONELY_BYTE_FILE_SIZE);
         assertReadByteWorkingCorrectly(dir, "LonelyByteInLastChunk.txt",
                  LAST_CHUNK_WITH_LONELY_BYTE_FILE_SIZE);
         assertReadBytesWorkingCorrectly(dir, "LonelyByteInLastChunk.txt",
                  LAST_CHUNK_WITH_LONELY_BYTE_FILE_SIZE, 12);
         assert 5 == getChunksNumber(cache, "index", "LonelyByteInLastChunk.txt");

      } finally {
         for (String fileName : dir.listAll()) {
            dir.deleteFile(fileName);
         }
         cache.getCacheManager().stop();
         dir.close();
      }
   }

   
   @Test
   public void testReadRandomSampleFile() throws IOException {
      final int BUFFER_SIZE = 64;

      Cache<CacheKey, Object> cache = CacheTestSupport.createTestCacheManager().getCache();
      InfinispanDirectory dir = new InfinispanDirectory(cache, "index", BUFFER_SIZE);

      try {
         final int FILE_SIZE = 1000;
         assert BUFFER_SIZE < FILE_SIZE;
         createFileWithRepeatableContent(dir, "RandomSampleFile.txt", FILE_SIZE);
         
         IndexInput indexInput = dir.openInput("RandomSampleFile.txt");
         assert indexInput.length() == FILE_SIZE;
         RepeatableLongByteSequence bytesGenerator = new RepeatableLongByteSequence();

         Random r = new Random();
         long seekPoint = 0;
         //Now it reads some random byte and it compares to the expected byte 
         for (int i = 0; i < FILE_SIZE; i++) {
            if(seekPoint == i) {
               assert bytesGenerator.nextByte() == indexInput.readByte();
               seekPoint = indexInput.getFilePointer() + r.nextInt(10);
               indexInput.seek(seekPoint);
            } else {
               bytesGenerator.nextByte();
            }
            
         }
         indexInput.close();

      } finally {
         for (String fileName : dir.listAll()) {
            dir.deleteFile(fileName);
         }
         cache.getCacheManager().stop();
         dir.close();
      }
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
   private void assertReadBytesWorkingCorrectly(InfinispanDirectory dir, String fileName,
            final int contentFileSizeExpected, final int arrayLengthToRead) throws IOException {
      IndexInput indexInput = dir.openInput(fileName);
      assert indexInput.length() == contentFileSizeExpected;

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
   private void assertReadByteWorkingCorrectly(InfinispanDirectory dir, String fileName,
            final int contentFileSizeExpected) throws IOException {
      IndexInput indexInput = dir.openInput(fileName);
      assert indexInput.length() == contentFileSizeExpected;
      RepeatableLongByteSequence bytesGenerator = new RepeatableLongByteSequence();

      for (int i = 0; i < contentFileSizeExpected; i++) {
         assert bytesGenerator.nextByte() == indexInput.readByte();
      }
      indexInput.close();
   }

   /**
    * It returns the number of chunks of file which is divided
    * 
    * @param cache
    * @param index
    * @param fileName
    * @return
    */
   private int getChunksNumber(Cache<CacheKey, Object> cache, String index, String fileName) {
      int chunksNumber = 0;
      while (cache.containsKey(new ChunkCacheKey(index, fileName, chunksNumber))) {
         chunksNumber++;
      }
      return chunksNumber;
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
   private void createFileWithRepeatableContent(InfinispanDirectory dir, String fileName,
            final int contentFileSize) throws IOException {
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

      Cache<CacheKey, Object> cache = CacheTestSupport.createTestCacheManager().getCache();
      InfinispanDirectory dir = new InfinispanDirectory(cache, "index", BUFFER_SIZE);

      // create file headers
      FileMetadata file1 = new FileMetadata();
      CacheKey key1 = new FileCacheKey("index", "Hello.txt");
      cache.put(key1, file1);

      FileMetadata file2 = new FileMetadata();
      CacheKey key2 = new FileCacheKey("index", "World.txt");
      cache.put(key2, file2);

      // byte array for Hello.txt
      String helloText = "Hello world.  This is some text.";
      cache.put(new ChunkCacheKey("index", "Hello.txt", 0), helloText.getBytes());

      // byte array for World.txt - should be in at least 2 chunks.
      String worldText = "This String should contain more than sixty four characters but less than one hundred and twenty eight.";

      byte[] buf = new byte[BUFFER_SIZE];
      System.arraycopy(worldText.getBytes(), 0, buf, 0, BUFFER_SIZE);
      cache.put(new ChunkCacheKey("index", "World.txt", 0), buf);

      String part1 = new String(buf);
      buf = new byte[BUFFER_SIZE];
      System.arraycopy(worldText.getBytes(), BUFFER_SIZE, buf, 0, worldText.length() - BUFFER_SIZE);
      cache.put(new ChunkCacheKey("index", "World.txt", 1), buf);
      String part2 = new String(buf);

      // make sure the generated bytes do add up!
      assert worldText.equals(part1 + part2.trim());

      file1.setSize(helloText.length());
      file2.setSize(worldText.length());

      Set<String> s = new HashSet<String>();
      s.add("Hello.txt");
      s.add("World.txt");
      Set other = new HashSet(Arrays.asList(dir.list()));

      // ok, file listing works.
      assert s.equals(other);

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
      assert null == cache.get(new FileCacheKey("index", "Hello.txt"));
      assert null == cache.get(new ChunkCacheKey("index", "Hello.txt", 0));

      Object ob1 = cache.get(new FileCacheKey("index", "World.txt"));
      Object ob2 = cache.get(new ChunkCacheKey("index", "World.txt", 0));
      Object ob3 = cache.get(new ChunkCacheKey("index", "World.txt", 1));

      dir.renameFile("World.txt", "HelloWorld.txt");
      assert null == cache.get(new FileCacheKey("index", "Hello.txt"));
      assert null == cache.get(new ChunkCacheKey("index", "Hello.txt", 0));
      assert null == cache.get(new ChunkCacheKey("index", "Hello.txt", 1));

      assert cache.get(new FileCacheKey("index", "HelloWorld.txt")).equals(ob1);
      assert cache.get(new ChunkCacheKey("index", "HelloWorld.txt", 0)).equals(ob2);
      assert cache.get(new ChunkCacheKey("index", "HelloWorld.txt", 1)).equals(ob3);

      // test that contents survive a move
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

      cache.getCacheManager().stop();
      dir.close();

   }

   public void testWriteChunks() throws Exception {
      final int BUFFER_SIZE = 64;

      Cache<CacheKey, Object> cache = CacheTestSupport.createTestCacheManager().getCache();
      InfinispanDirectory dir = new InfinispanDirectory(cache, "index", BUFFER_SIZE);

      IndexOutput io = dir.createOutput("MyNewFile.txt");

      io.writeByte((byte) 66);
      io.writeByte((byte) 69);

      io.close();

      assert dir.fileExists("MyNewFile.txt");
      assert null != cache.get(new ChunkCacheKey("index", "MyNewFile.txt", 0));

      // test contents by reading:
      byte[] buf = new byte[9];
      IndexInput ii = dir.openInput("MyNewFile.txt");
      ii.readBytes(buf, 0, (int) ii.length());

      assert new String(new byte[] { 66, 69 }).equals(new String(buf).trim());

      String testText = "This is some rubbish again that will span more than one chunk - one hopes.  Who knows, maybe even three or four chunks.";
      io.seek(0);
      io.writeBytes(testText.getBytes(), 0, testText.length());
      io.close();
      // now compare.
      byte[] chunk1 = (byte[]) cache.get(new ChunkCacheKey("index", "MyNewFile.txt", 0));
      byte[] chunk2 = (byte[]) cache.get(new ChunkCacheKey("index", "MyNewFile.txt", 1));
      assert null != chunk1;
      assert null != chunk2;

      assert testText.equals(new String(chunk1) + new String(chunk2).trim());

      cache.getCacheManager().stop();
      dir.close();
   }

   public void testWriteChunksDefaultChunks() throws Exception {
      Cache<CacheKey, Object> cache = CacheTestSupport.createTestCacheManager().getCache();
      InfinispanDirectory dir = new InfinispanDirectory(cache, "index");

      String testText = "This is some rubbish";
      byte[] testTextAsBytes = testText.getBytes();

      IndexOutput io = dir.createOutput("MyNewFile.txt");

      io.writeByte((byte) 1);
      io.writeByte((byte) 2);
      io.writeByte((byte) 3);
      io.writeBytes(testTextAsBytes, testTextAsBytes.length);
      io.close();

      assert null != cache.get(new FileCacheKey("index", "MyNewFile.txt"));
      assert null != cache.get(new ChunkCacheKey("index", "MyNewFile.txt", 0));

      // test contents by reading:
      IndexInput ii = dir.openInput("MyNewFile.txt");
      assert ii.readByte() == 1;
      assert ii.readByte() == 2;
      assert ii.readByte() == 3;
      byte[] buf = new byte[32];

      ii.readBytes(buf, 0, testTextAsBytes.length);

      assert testText.equals(new String(buf).trim());

      cache.getCacheManager().stop();
      dir.close();
   }

}
