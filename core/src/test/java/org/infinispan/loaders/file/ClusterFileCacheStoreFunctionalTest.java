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
package org.infinispan.loaders.file;

import java.io.File;
import java.lang.reflect.Method;

import javax.transaction.TransactionManager;

import org.infinispan.Cache;
import org.infinispan.atomic.AtomicMap;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import static org.infinispan.atomic.AtomicMapLookup.getAtomicMap;
import static org.infinispan.test.fwk.TestCacheManagerFactory.createClusteredCacheManager;
import static org.testng.AssertJUnit.assertEquals;

/**
 * @author Galder Zamarreño
 * @since 4.1
 */
@Test(groups = "unit", testName = "loaders.file.ClusterFileCacheStoreFunctionalTest")
public class ClusterFileCacheStoreFunctionalTest extends MultipleCacheManagersTest {

   // createCacheManager executes before any @BeforeClass defined in the class, so simply use standard tmp folder.
   private final String tmpDirectory = TestingUtil.tmpDirectory(this);
   private static final String CACHE_NAME = "clusteredFileCacheStore";

   private Cache<Object, ?> cache1, cache2;

   @AfterClass
   protected void clearTempDir() {
      TestingUtil.recursiveFileRemove(tmpDirectory);
      new File(tmpDirectory).mkdirs();
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      EmbeddedCacheManager cacheManager1 = createClusteredCacheManager(GlobalConfigurationBuilder.defaultClusteredBuilder(),
                                                                       new ConfigurationBuilder());
      EmbeddedCacheManager cacheManager2 = createClusteredCacheManager(GlobalConfigurationBuilder.defaultClusteredBuilder(),
                                                                       new ConfigurationBuilder());
      registerCacheManager(cacheManager1, cacheManager2);

      ConfigurationBuilder builder1 = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, true);
      addCacheStoreConfig(builder1, 1);
      ConfigurationBuilder builder2 = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, true);
      addCacheStoreConfig(builder2, 2);

      cacheManager1.defineConfiguration(CACHE_NAME, builder1.build());
      cacheManager2.defineConfiguration(CACHE_NAME, builder2.build());
      cache1 = cache(0, CACHE_NAME);
      cache2 = cache(1, CACHE_NAME);
   }

   public void testRestoreTransactionalAtomicMap(Method m) throws Exception {
      final Object mapKey = m.getName();
      TransactionManager tm = cache1.getAdvancedCache().getTransactionManager();
      tm.begin();
      final AtomicMap<String, String> map = getAtomicMap(cache1, mapKey);
      map.put("a", "b");
      tm.commit();

      //ISPN-3161 => the eviction tries to acquire the lock, however the TxCompletionNotificationCommand is sent async
      //             and the deliver can be delayed resulting in a delay releasing the lock and a TimeoutException
      //             when the evict tries to acquire the lock.
      assertEventuallyNoLocksAcquired(mapKey);

      //evict from memory
      cache1.evict(mapKey);

      // now re-retrieve the map and make sure we see the diffs
      assertEquals("Wrong value for key [a] in atomic map.", "b", getAtomicMap(cache1, mapKey).get("a"));
      assertEquals("Wrong value for key [a] in atomic map.", "b", getAtomicMap(cache2, mapKey).get("a"));

      cache2.evict(mapKey);
      assertEquals("Wrong value for key [a] in atomic map.", "b", getAtomicMap(cache1, mapKey).get("a"));
      assertEquals("Wrong value for key [a] in atomic map.", "b", getAtomicMap(cache2, mapKey).get("a"));
   }

   protected void addCacheStoreConfig(ConfigurationBuilder builder, int index) {
      builder.loaders().addFileCacheStore().location(tmpDirectory + "/" + index).purgeSynchronously(true);
   }

   protected void assertEventuallyNoLocksAcquired(final Object key) {
      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return !TestingUtil.extractLockManager(cache1).isLocked(key);
         }
      });
      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return !TestingUtil.extractLockManager(cache2).isLocked(key);
         }
      });
   }

}
