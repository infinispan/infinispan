/*
 * Copyright 2012 Red Hat, Inc. and/or its affiliates.
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

package org.infinispan.query.blackbox;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.CacheManagerCallable;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import static org.infinispan.test.TestingUtil.withCacheManager;

/**
 * Tests whether query caches can restart without problems.
 *
 * @author Galder Zamarreño
 * @since 5.2
 */
@Test(testName = "query.blackbox.QueryCacheRestartTest", groups = "functional")
public class QueryCacheRestartTest extends AbstractInfinispanTest {

   public void testQueryCacheRestart() {
      queryCacheRestart(false);
   }

   public void testLocalQueryCacheRestart() {
      queryCacheRestart(true);
   }

   private void queryCacheRestart(boolean localOnly) {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.indexing().enable().indexLocalOnly(localOnly)
            .addProperty("hibernate.search.default.directory_provider", "ram")
            .addProperty("hibernate.search.lucene_version", "LUCENE_CURRENT");
      withCacheManager(new CacheManagerCallable(TestCacheManagerFactory.createCacheManager(builder.build())) {
         @Override
         public void call() {
            Cache<Object,Object> cache = cm.getCache();
            cache.stop();
            cache.start();
         }
      });
   }

}
