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
package org.infinispan.config.parsing;

import org.infinispan.Cache;
import org.infinispan.config.CacheLoaderManagerConfig;
import org.infinispan.config.Configuration;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.loaders.CacheLoaderConfig;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;

import static java.lang.Thread.currentThread;
import static org.testng.Assert.assertEquals;

/**
 * @author Mircea.Markus@jboss.com
 */
@Test(groups = "functional", testName = "config.parsing.EHCache2InfinispanTransformerTest")
public class EHCache2InfinispanTransformerTest extends AbstractInfinispanTest {

   private static final String BASE_DIR = "configs/ehcache";
   ConfigFilesConvertor convertor = new ConfigFilesConvertor();

   public void testEhCache16File() throws Exception {
      testAllFile("/ehcache-1.6.xml");
   }

//   @Test(enabled=false)
   public void testEhCache15File() throws Exception {
      testAllFile("/ehcache-1.5.xml");
   }

   /**
    * Transforms and tests the transformation of a complex file.
    */
   private void testAllFile(String ehCacheFile) throws Exception {
      ClassLoader existingCl = currentThread().getContextClassLoader();
      DefaultCacheManager dcm = null;
      Cache<Object, Object> sampleDistributedCache2 = null;
      try {
         ClassLoader delegatingCl = new Jbc2InfinispanTransformerTest.TestClassLoader(existingCl);
         currentThread().setContextClassLoader(delegatingCl);
         String fileName = getFileName(ehCacheFile);
         ByteArrayOutputStream baos = new ByteArrayOutputStream();
         convertor.parse(fileName, baos, ConfigFilesConvertor.TRANSFORMATIONS.get(ConfigFilesConvertor.EHCACHE_CACHE1X), Thread.currentThread().getContextClassLoader());
         
         dcm = (DefaultCacheManager) TestCacheManagerFactory.fromStream(new ByteArrayInputStream(baos.toByteArray()));
         Cache<Object,Object> defaultCache = dcm.getCache();
         defaultCache.put("key", "value");
         Configuration configuration = defaultCache.getConfiguration();
         assertEquals(configuration.getEvictionMaxEntries(),10000);
         assertEquals(configuration.getExpirationMaxIdle(), 121);
         assertEquals(configuration.getExpirationLifespan(), 122);
         CacheLoaderManagerConfig clmConfig = configuration.getCacheLoaderManagerConfig();
         assert clmConfig != null;
         CacheLoaderConfig loaderConfig = clmConfig.getCacheLoaderConfigs().get(0);
         assert loaderConfig.getCacheLoaderClassName().equals("org.infinispan.loaders.file.FileCacheStore");
         assertEquals(configuration.getExpirationWakeUpInterval(), 119000);
         assertEquals(configuration.getEvictionStrategy(), EvictionStrategy.LRU);

         assert dcm.getDefinedCacheNames().contains("sampleCache1");
         assert dcm.getDefinedCacheNames().contains("sampleCache2");
         assert dcm.getDefinedCacheNames().contains("sampleCache3");
         assert dcm.getDefinedCacheNames().contains("sampleDistributedCache1");
         assert dcm.getDefinedCacheNames().contains("sampleDistributedCache2");
         assert dcm.getDefinedCacheNames().contains("sampleDistributedCache3");

         sampleDistributedCache2 = dcm.getCache("sampleDistributedCache2");
         assert sampleDistributedCache2.getConfiguration().getCacheLoaderManagerConfig().getCacheLoaderConfigs().size() == 1;
         assert sampleDistributedCache2.getConfiguration().getExpirationLifespan() == 101;
         assert sampleDistributedCache2.getConfiguration().getExpirationMaxIdle() == 102;
         assertEquals(sampleDistributedCache2.getConfiguration().getCacheMode(), Configuration.CacheMode.INVALIDATION_SYNC);

      } finally {
         currentThread().setContextClassLoader(existingCl);
         TestingUtil.killCaches(sampleDistributedCache2);
         TestingUtil.killCacheManagers(dcm);
      }
   }

   private String getFileName(String s) {
      return BASE_DIR + File.separator + s;
   }
}
