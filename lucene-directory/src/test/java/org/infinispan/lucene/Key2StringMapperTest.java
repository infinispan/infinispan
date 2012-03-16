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
