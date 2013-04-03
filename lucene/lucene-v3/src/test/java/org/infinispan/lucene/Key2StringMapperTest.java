/*
 * JBoss, Home of Professional Open Source
 * Copyright 2010 Red Hat Inc. and/or its affiliates and other
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

import java.util.Arrays;

import org.testng.AssertJUnit;
import org.testng.annotations.Test;

/**
 * Tests basic functionality of LuceneKey2StringMapper
 * @see LuceneKey2StringMapper
 * 
 * @author Sanne Grinovero
 * @since 4.1
 */
@Test(groups = "functional", testName = "lucene.Key2StringMapperTest")
public class Key2StringMapperTest {
   
   final LuceneKey2StringMapper mapper = new LuceneKey2StringMapper();
   
   @Test
   public void testRegex() {
      String[] split = LuceneKey2StringMapper.singlePipePattern.split("hello|world");
      AssertJUnit.assertTrue(Arrays.deepEquals(new String[]{"hello", "world"}, split));
   }
   
   @Test
   public void loadChunkCacheKey() {
      AssertJUnit.assertEquals(new ChunkCacheKey("my addressbook", "sgments0.gen", 34, 16000000), mapper.getKeyMapping("sgments0.gen|34|16000000|my addressbook"));
   }
   
   @Test
   public void loadFileCacheKey() {
      AssertJUnit.assertEquals(new FileCacheKey("poems and songs, 3000AC-2000DC", "filename.extension"), mapper.getKeyMapping("filename.extension|M|poems and songs, 3000AC-2000DC"));
   }
   
   @Test
   public void loadFileListCacheKey() {
      AssertJUnit.assertEquals(new FileListCacheKey(""), mapper.getKeyMapping("*|"));
      AssertJUnit.assertEquals(new FileListCacheKey("the leaves of Amazonia"), mapper.getKeyMapping("*|the leaves of Amazonia"));
   }
   
   @Test
   public void loadReadLockKey() {
      AssertJUnit.assertEquals(new FileReadLockKey("poems and songs, 3000AC-2000DC", "brushed steel lock"), mapper.getKeyMapping("brushed steel lock|RL|poems and songs, 3000AC-2000DC"));
   }
   
   @Test(expectedExceptions=IllegalArgumentException.class)
   public void failureForIllegalKeys() {
      mapper.getKeyMapping("|*|the leaves of Amazonia");
   }

   @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Not supporting null keys")
   public void failureForNullKey() {
      mapper.getKeyMapping(null);
   }

   @Test(expectedExceptions = IllegalArgumentException.class)
   public void failureForNotFullKey() {
      mapper.getKeyMapping("sgments0.gen|34");
   }

   @Test(expectedExceptions = IllegalArgumentException.class)
   public void failureForWrongFileCacheKey() {
      mapper.getKeyMapping("filename|M|5|indexname");
   }

   @Test(expectedExceptions = IllegalArgumentException.class)
   public void failureForWrongChunkCacheKey() {
      mapper.getKeyMapping("filename|5a|5|indexname");
   }

   @Test(expectedExceptions = IllegalArgumentException.class)
   public void failureForWrongFileReadLockKey() {
      mapper.getKeyMapping("filename|RL|5|indexname");
   }

   @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "filename must not be null")
   public void testChunkCacheKeyInitWithNllFileName() {
      new ChunkCacheKey("index-A", null, 0, 1024);
   }

   @Test
   public void testChunkCacheKeyComparison() {
      AssertJUnit.assertFalse("The object should not be equals to null.", new ChunkCacheKey("index-A", "fileName", 0, 1024).equals(null));
      AssertJUnit.assertFalse("The ChunkCacheKey objects should not be equal due to different file names.",
                         new ChunkCacheKey("index-A", "fileName", 0, 1024).equals(new ChunkCacheKey("index-A", "fileName1", 0, 1024)));
      AssertJUnit.assertEquals("The ChunkCacheKey objects should be equal.",
                        new ChunkCacheKey("index-A", "fileName", 0, 1024), new ChunkCacheKey("index-A", "fileName", 0, 1024));
   }

   public void testIsSupportedType() {
      assert !mapper.isSupportedType(this.getClass());
      assert mapper.isSupportedType(ChunkCacheKey.class);
      assert mapper.isSupportedType(FileCacheKey.class);
      assert mapper.isSupportedType(FileListCacheKey.class);
      assert mapper.isSupportedType(FileReadLockKey.class);
   }

   @Test(expectedExceptions=IllegalArgumentException.class)
   public void testReadLockKeyIndexNameNull() {
      FileReadLockKey key = new FileReadLockKey(null, "brushed steel lock");
   }

   @Test(expectedExceptions = IllegalArgumentException.class)
   public void testReadLockKeyFileNameNull() {
      FileReadLockKey key = new FileReadLockKey("poems and songs, 3000AC-2000DC", null);
   }

   public void testReadLockEqualsWithNullOrNotEqualObj() {
      FileReadLockKey key = new FileReadLockKey("poems and songs, 3000AC-2000DC", "brushed steel lock");
      assert !key.equals(null);

      AssertJUnit.assertFalse(new FileReadLockKey("poems and songs, 3000AC-2000DC", "brushed lock")
                     .equals(mapper.getKeyMapping("brushed steel lock|RL|poems and songs, 3000AC-2000DC")));
   }

   @Test
   public void testFileListCacheKeyComparison() {
      AssertJUnit.assertFalse("The instance of FileListCacheKey should not be equal to null.", new FileListCacheKey("index-A").equals(null));
      AssertJUnit.assertFalse("The instance of FileListCacheKey should not be equal to FileCacheKey instance.", new FileListCacheKey("index-A").equals(new FileCacheKey("index-A", "test.txt")));
   }

   @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "filename must not be null")
   public void testFileCacheKeyInit() {
      new FileCacheKey("poems and songs, 3000AC-2000DC", null);
   }

   @Test
   public void testFileCacheKeyCompWithNull() {
      AssertJUnit.assertFalse("The Instance of FileCacheKey should not be equal to null.", new FileCacheKey("poems and songs, 3000AC-2000DC", "fileName.txt").equals(null));
   }
}
