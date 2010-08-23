package org.infinispan.lucene;

import java.util.Arrays;

import junit.framework.Assert;

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
      Assert.assertTrue(Arrays.deepEquals(new String[] { "hello", "world" }, split));
   }
   
   @Test
   public void loadChunkCacheKey() {
      Assert.assertEquals(new ChunkCacheKey("my addressbook", "sgments0.gen", 34), mapper.getKeyMapping("sgments0.gen|34|my addressbook"));
   }
   
   @Test
   public void loadFileCacheKey() {
      Assert.assertEquals(new FileCacheKey("poems and songs, 3000AC-2000DC", "filename.extension"), mapper.getKeyMapping("filename.extension|M|poems and songs, 3000AC-2000DC"));
   }
   
   @Test
   public void loadFileListCacheKey() {
      Assert.assertEquals(new FileListCacheKey(""), mapper.getKeyMapping("*|"));
      Assert.assertEquals(new FileListCacheKey("the leaves of Amazonia"), mapper.getKeyMapping("*|the leaves of Amazonia"));
   }
   
   @Test
   public void loadReadLockKey() {
      Assert.assertEquals(new FileReadLockKey("poems and songs, 3000AC-2000DC", "brushed steel lock"), mapper.getKeyMapping("brushed steel lock|RL|poems and songs, 3000AC-2000DC"));
   }
   
   @Test(expectedExceptions=IllegalArgumentException.class)
   public void failureForIllegalKeys(){
      mapper.getKeyMapping("|*|the leaves of Amazonia");
   }

}
