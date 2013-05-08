/*
 * JBoss, Home of Professional Open Source
 * Copyright 2013 Red Hat Inc. and/or its affiliates and other
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

package org.infinispan.query.blackbox;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import java.io.File;

/**
 * Run the basic set of operations with filesystem-based index storage in replicated mode
 * with transactional caches.
 *
 * The default FSDirectory implementation for non Windows systems should be NIOFSDirectory.
 * SimpleFSDirectory implementation will be used on Windows.
 *
 * @author Martin Gencur
 */
@Test(groups = "functional", testName = "query.blackbox.ClusteredCacheFSDirectoryTest")
public class ClusteredCacheFSDirectoryTest extends ClusteredCacheTest {

   private String TMP_DIR;

   @Override
   protected void createCacheManagers() throws Exception {
      addClusterEnabledCacheManager(buildCacheConfig("index1"));
      addClusterEnabledCacheManager(buildCacheConfig("index2"));
      waitForClusterToForm();
      cache1 = cache(0);
      cache2 = cache(1);
   }

   private ConfigurationBuilder buildCacheConfig(String indexName) {
      ConfigurationBuilder cb = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, true);
      cb.indexing()
            .enable()
            .indexLocalOnly(false) //index also changes originated on other nodes, the index is not shared
            .addProperty("default.directory_provider", "filesystem")
            .addProperty("default.indexBase", TMP_DIR + File.separator + indexName)
            .addProperty("lucene_version", "LUCENE_CURRENT");
      return cb;
   }

   @BeforeMethod
   protected void setUpTempDir() {
      TMP_DIR = TestingUtil.tmpDirectory(this);
      new File(TMP_DIR).mkdirs();
   }

   @Override
   @AfterMethod
   protected void clearContent() throws Throwable {
      try {
         //first stop cache managers, then clear the index
         super.clearContent();
      } finally {
         //delete the index otherwise it will mess up the index for next tests
         TestingUtil.recursiveFileRemove(TMP_DIR);
      }
   }
}
