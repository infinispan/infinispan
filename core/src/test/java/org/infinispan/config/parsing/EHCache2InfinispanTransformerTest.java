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
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.FileCacheStoreConfiguration;
import org.infinispan.configuration.cache.LoadersConfiguration;
import org.infinispan.eviction.EvictionStrategy;
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
 * @author Tristan Tarrant
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
         Configuration configuration = defaultCache.getCacheConfiguration();

         assertEquals(configuration.eviction().maxEntries(),10000);
         assertEquals(configuration.expiration().maxIdle(), 121);
         assertEquals(configuration.expiration().lifespan(), 122);
         LoadersConfiguration loaders = configuration.loaders();
         assert loaders.cacheLoaders().get(0) instanceof FileCacheStoreConfiguration;

         assertEquals(configuration.expiration().wakeUpInterval(), 119000);
         assertEquals(configuration.eviction().strategy(), EvictionStrategy.LRU);

         String definedCacheNames = dcm.getDefinedCacheNames();
         assert definedCacheNames.contains("sampleCache1");
         assert definedCacheNames.contains("sampleCache2");
         assert definedCacheNames.contains("sampleCache3");
         assert definedCacheNames.contains("sampleDistributedCache1");
         assert definedCacheNames.contains("sampleDistributedCache2");
         assert definedCacheNames.contains("sampleDistributedCache3");

         sampleDistributedCache2 = dcm.getCache("sampleDistributedCache2");
         Configuration configuration2 = sampleDistributedCache2.getCacheConfiguration();
         assert configuration2.loaders().cacheLoaders().size() == 1;
         assert configuration2.expiration().lifespan() == 101;
         assert configuration2.expiration().maxIdle() == 102;
         assertEquals(configuration2.clustering().cacheMode(), CacheMode.INVALIDATION_SYNC);

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
