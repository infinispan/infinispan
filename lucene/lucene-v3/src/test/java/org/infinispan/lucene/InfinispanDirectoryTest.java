/*
 * JBoss, Home of Professional Open Source
 * Copyright 2013 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */
package org.infinispan.lucene;

import junit.framework.Assert;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockFactory;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.lucene.readlocks.DistributedSegmentReadLocker;
import org.infinispan.lucene.readlocks.SegmentReadLocker;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

import java.io.IOException;

import static org.infinispan.lucene.CacheTestSupport.assertTextIsFoundInIds;
import static org.infinispan.lucene.CacheTestSupport.writeTextToIndex;

/**
 * Tests covering InfinispanDirectory simple use cases, like:
 *   - InfinispanDirectory object initialization with illegal arguments.
 *   - InfinispanDirectory object initialization with proper arguments.
 *   - Tests are added testing the touchFile, fileModified, renameFile methods.
 *
 * @author Anna Manukyan
 */
@Test(groups = "functional", testName = "lucene.InfinispanDirectoryTest")
public class InfinispanDirectoryTest extends SingleCacheManagerTest {


   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder configuration = CacheTestSupport.createLocalCacheConfiguration();

      return TestCacheManagerFactory.createCacheManager(configuration);
   }

   @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "chunkSize must be a positive integer")
   public void testInitWithInvalidChunkSize() {
      Cache cache = cacheManager.getCache();

      InfinispanDirectory dir = new InfinispanDirectory(cache, "index", 0, new DistributedSegmentReadLocker(cache, cache, cache, "index"));
   }

   @Test(expectedExceptions = IllegalArgumentException.class)
   public void testInitWithInvalidCache() {
      Cache cache = cacheManager.getCache();

      InfinispanDirectory dir = new InfinispanDirectory(null, null, "cachename", null, 10, null);
   }

   @Test(expectedExceptions = IllegalArgumentException.class)
   public void testInitWithInvalidChunkCache() {
      Cache cache = cacheManager.getCache();

      InfinispanDirectory dir = new InfinispanDirectory(cache, null, "cachename", null, 10, null);
   }

   @Test(expectedExceptions = IllegalArgumentException.class)
   public void testInitWithInvalidIndexName() {
      Cache cache = cacheManager.getCache();

      InfinispanDirectory dir = new InfinispanDirectory(cache, cache, null, null, 10, null);
   }

   @Test(expectedExceptions = IllegalArgumentException.class)
   public void testInitWithInvalidLockFactory() {
      Cache cache = cacheManager.getCache();

      InfinispanDirectory dir = new InfinispanDirectory(cache, cache, "indexName", null, 10, null);
   }

   @Test(expectedExceptions = IllegalArgumentException.class)
   public void testInitWithInvalidSegmentReadLocker() {
      Cache cache = cacheManager.getCache();

      InfinispanDirectory dir = new InfinispanDirectory(cache, cache, "indexName", new LockFactory() {
         @Override
         public Lock makeLock(String lockName) {
            return null;
         }

         @Override
         public void clearLock(String lockName) throws IOException {
         }
      }, 10, null);
   }

   @Test
   public void testInitWithConstructor1() throws Exception {
      Cache cache = cacheManager.getCache();

      Directory dir = new InfinispanDirectory(cache, "index", 10, new DistributedSegmentReadLocker(cache, cache, cache, "index"));
      verifyDir(dir, "index");
      dir.close();
   }

   @Test
   public void testInitWithConstructor2() throws Exception {
      Cache cache = cacheManager.getCache();

      Directory dir = new InfinispanDirectory(cache, "index");
      verifyDir(dir, "index");
      dir.close();
   }

   @Test
   public void testInitWithConstructor3() throws Exception {
      Cache cache = cacheManager.getCache();

      Directory dir = new InfinispanDirectory(cache);
      verifyDir(dir, "");
      dir.close();
   }

   @Test
   public void testFileModified() throws Exception {
      Cache cache = cacheManager.getCache();
      String fileName = "dummyFileName";

      Directory dir = new InfinispanDirectory(cache, "index");
      createFile(fileName, dir);

      assert dir.fileExists(fileName);
      assert dir.fileModified(fileName) != 0;

      assert dir.fileModified("nonExistentFileName.txt") == 0;

      dir.close();
   }

   @Test
   public void testTouchFile() throws Exception {
      Cache cache = cacheManager.getCache();
      String fileName = "testfile.txt";

      Directory dir = new InfinispanDirectory(cache, "index");
      createFile(fileName, dir);

      long lastModifiedDate = dir.fileModified(fileName);

      Thread.sleep(100);

      dir.touchFile(fileName);
      assert lastModifiedDate != dir.fileModified(fileName);

      dir.close();
   }

   @Test
   public void testTouchNonExistentFile() throws Exception {
      Cache cache = cacheManager.getCache();
      String fileName = "nonExistent.txt";

      Directory dir = new InfinispanDirectory(cache, "index");

      long lastModifiedDate = dir.fileModified(fileName);
      Thread.sleep(100);

      dir.touchFile(fileName);
      Assert.assertEquals(lastModifiedDate, dir.fileModified(fileName));

      dir.close();
   }

   @Test
   public void testRenameFile() throws Exception {
      Cache cache = cacheManager.getCache();
      String fileName = "testfile.txt";
      String newFileName = "newtestfile.txt";

      Directory dir = new InfinispanDirectory(cache, "index");
      createFile(fileName, dir);

      ((InfinispanDirectory) dir).renameFile(fileName, newFileName);

      assert !dir.fileExists(fileName);
      assert dir.fileExists(newFileName);

      dir.close();
   }

   @Test
   public void testFileLength() throws IOException {
      Directory dir = new InfinispanDirectory(cache, "index");

      Assert.assertEquals(0, dir.fileLength("nonExistentFile.txt"));
   }

   private void createFile(final String fileName, final Directory dir) throws IOException {
      IndexOutput io = dir.createOutput(fileName);

      io.writeByte((byte) 66);
      io.writeByte((byte) 69);

      io.flush();
      io.close();
   }
   private void verifyDir(final Directory dir, final String expectedIndexName) throws IOException {
      InfinispanDirectory infDir = (InfinispanDirectory) dir;
      AssertJUnit.assertEquals(expectedIndexName, infDir.getIndexName());

      writeTextToIndex(dir, 0, "hi all");
      assertTextIsFoundInIds(dir, "hi", 0);
      writeTextToIndex(dir, 1, "all together");
      assertTextIsFoundInIds(dir, "all", 0, 1);
   }
}
