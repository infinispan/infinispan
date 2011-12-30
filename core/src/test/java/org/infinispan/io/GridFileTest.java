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
import java.io.IOException;

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

   public void testNonExistentFileIsNeitherFileNorDirectory() throws IOException {
      File file = fs.getFile("nonExistentFile.txt");
      assertFalse(file.exists());
      assertFalse(file.isFile());
      assertFalse(file.isDirectory());
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

}
