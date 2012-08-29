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

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * // TODO: Document this
 *
 * @author Galder Zamarreño
 * @since // TODO
 */
@Test(testName = "query.blackbox.QueryCacheRestartTest", groups = "functional")
public class QueryCacheRestartTest extends SingleCacheManagerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder builder = getDefaultStandaloneCacheConfig(true);
      builder.indexing().enable().indexLocalOnly(false)
         .addProperty("hibernate.search.default.directory_provider", "ram")
         .addProperty("hibernate.search.lucene_version", "LUCENE_CURRENT");
      return TestCacheManagerFactory.createCacheManager(builder.build());
   }

   public void testQueryCacheRestart() {
      cache.stop();
      cache.start();
   }

}
