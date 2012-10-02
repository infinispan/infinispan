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

package org.infinispan.query.cacheloaders;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.testng.annotations.Test;

/**
 * @author anistor@redhat.com
 * @since 5.2
 */
@Test(groups = "functional", testName = "query.cacheloaders.SharedCacheLoaderQueryDistributedIndexTest", enabled = false,
      description = "Temporary disabled: https://issues.jboss.org/browse/ISPN-2249 , https://issues.jboss.org/browse/ISPN-1586")
public class SharedCacheLoaderQueryDistributedIndexTest extends SharedCacheLoaderQueryIndexTest {

   @Override
   protected void configureCache(ConfigurationBuilder builder) {
      super.configureCache(builder);

      builder.indexing().enable().indexLocalOnly(true)
            .addProperty("hibernate.search.default.directory_provider", "infinispan")
            .addProperty("hibernate.search.default.indexmanager", "org.infinispan.query.indexmanager.InfinispanIndexManager")
            .addProperty("hibernate.search.lucene_version", "LUCENE_36")
            .addProperty("hibernate.search.default.exclusive_index_use", "false");
   }
}
