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
import org.infinispan.loaders.CacheStore;
import org.infinispan.loaders.dummy.DummyInMemoryCacheStore;
import org.infinispan.query.statetransfer.BaseReIndexingTest;
import org.infinispan.query.test.Person;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

/**
 * Tests behaviour of indexing and querying when a cache is clustered and
 * and it's configured with a shared cache store. If preload is enabled,
 * it should be possible to index the preloaded contents.
 *
 * @author Galder Zamarreño
 * @since 5.2
 */
@Test(groups = "functional", testName = "query.cacheloaders.SharedCacheLoaderQueryIndexTest", enabled = false,
      description = "Temporary disabled: https://issues.jboss.org/browse/ISPN-2249 , https://issues.jboss.org/browse/ISPN-1586")
public class SharedCacheLoaderQueryIndexTest extends BaseReIndexingTest {

   protected void configureCache(ConfigurationBuilder builder) {
      // To force a shared cache store, make sure storeName property
      // for dummy store is the same for all nodes
      builder.clustering().stateTransfer().fetchInMemoryState(false)
         .loaders().shared(true).preload(true).addStore()
            .cacheStore(new DummyInMemoryCacheStore()).addProperty("storeName", getClass().getName());
   }

   public void testPreloadIndexingAfterAddingNewNode() throws Exception {
      loadCacheEntries(this.<String, Person>caches().get(0));

      for (CacheStore cs: TestingUtil.cachestores(this.<String, Person>caches())) {
         assert cs.containsKey(persons[0].getName()) :
               "Cache misconfigured, maybe cache store not pointing to same place, maybe passivation on...etc";
         DummyInMemoryCacheStore dimcs = (DummyInMemoryCacheStore) cs;
         assert dimcs.stats().get("clear") == 0:
               "Cache store should not be cleared, purgeOnStartup is false";
         assert dimcs.stats().get("store") == 4:
               "Cache store should have been written to 4 times, but was written to " + dimcs.stats().get("store") + " times";
      }

      // Before adding a node, verify that the query resolves properly
      executeSimpleQuery(this.<String, Person>caches().get(0));

      addNodeCheckingContentsAndQuery();
   }

}
