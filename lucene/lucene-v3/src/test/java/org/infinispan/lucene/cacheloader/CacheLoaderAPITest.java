/* 
 * JBoss, Home of Professional Open Source
 * Copyright 2013 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @authors tag. All rights reserved.
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

package org.infinispan.lucene.cacheloader;

import java.io.File;

import org.apache.lucene.store.FSDirectory;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.loaders.CacheLoader;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.CacheLoaderManager;
import org.infinispan.lucene.cachestore.LuceneCacheLoader;
import org.infinispan.lucene.cachestore.LuceneCacheLoaderConfig;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * @author Sanne Grinovero <sanne@hibernate.org> (C) 2013 Red Hat Inc.
 * @since 5.2
 */
@Test(groups = "functional", testName = "lucene.cacheloader.CacheLoaderAPITest")
public class CacheLoaderAPITest extends SingleCacheManagerTest {

   private static final String rootDirectoryName = "CacheLoaderAPITest.indexesRootDirTmp";

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      File rootDir = new File(new File("."), rootDirectoryName);
      File subDir = new File(rootDir, "indexNameOne");
      boolean directoriesCreated = subDir.mkdirs();
      assert directoriesCreated : "couldn't create directory for test";
      //We need at least one Directory to exist on filesystem to trigger the problem
      FSDirectory luceneDirectory = FSDirectory.open(subDir);
      luceneDirectory.close();
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder
         .loaders()
            .addLoader()
               .cacheLoader( new LuceneCacheLoader() )
                  .addProperty(LuceneCacheLoaderConfig.LOCATION_OPTION, rootDir.getAbsolutePath())
                  .addProperty(LuceneCacheLoaderConfig.AUTO_CHUNK_SIZE_OPTION, "1024");
      return TestCacheManagerFactory.createCacheManager(builder);
   }

   public void filteredKeyLoadTest() throws CacheLoaderException {
      CacheLoaderManager cacheLoaderManager = TestingUtil.extractComponent(cache, CacheLoaderManager.class);
      CacheLoader loader = cacheLoaderManager.getCacheLoader();
      Assert.assertNotNull(loader);
      Assert.assertTrue(loader instanceof LuceneCacheLoader);
      LuceneCacheLoader cacheLoader = (LuceneCacheLoader) loader;
      cacheLoader.loadAllKeys(null);
   }

   @Override
   protected void teardown() {
      super.teardown();
      File rootDir = new File(new File("."), rootDirectoryName);
      TestingUtil.recursiveFileRemove(rootDir);
   }

}
