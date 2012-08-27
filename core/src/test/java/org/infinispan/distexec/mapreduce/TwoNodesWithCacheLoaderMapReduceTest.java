/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other
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
package org.infinispan.distexec.mapreduce;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.LegacyStoreConfigurationBuilder;
import org.infinispan.loaders.dummy.DummyInMemoryCacheStore;
import org.testng.annotations.Test;

/**
 * TwoNodesWithCacheLoaderMapReduceTest tests Map/Reduce functionality using two Infinispan nodes,
 * local reduce and also to verify that having values in cacheloader as well does not lead to any
 * additional key/value inclusion in map/reduce algorithm
 *
 * @author Vladimir Blagojevic
 * @since 5.2
 */
@Test(groups = "functional", testName = "distexec.TwoNodesWithCacheLoaderMapReduceTest")
public class TwoNodesWithCacheLoaderMapReduceTest extends BaseWordCountMapReduceTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(getCacheMode(), true);
      LegacyStoreConfigurationBuilder store = builder.loaders().addStore().cacheStore(new DummyInMemoryCacheStore(getClass().getSimpleName()));
      store.purgeOnStartup(true);
      createClusteredCaches(2, cacheName(), builder);
   }
}
