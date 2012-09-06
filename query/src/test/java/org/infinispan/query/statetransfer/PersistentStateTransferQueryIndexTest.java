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

package org.infinispan.query.statetransfer;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.CacheLoaderManager;
import org.infinispan.loaders.CacheStore;
import org.infinispan.loaders.dummy.DummyInMemoryCacheStore;
import org.infinispan.query.test.Person;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.assertEquals;

/**
 * Test that verifies that querying works even after multiple nodes have
 * started with unshared, passivated, cache stores, and a new node comes in
 * to fetch the persistent state from the other nodes.
 *
 * @author Galder Zamarreño
 * @since 5.2
 */
@Test(groups = "functional", testName = "query.statetransfer.PersistentStateTransferQueryIndexTest")
public class PersistentStateTransferQueryIndexTest extends BaseReIndexingTest {

   @Override
   protected void configureCache(ConfigurationBuilder builder) {
      builder.clustering().stateTransfer().fetchInMemoryState(false)
            .loaders().passivation(true).shared(false).addStore()
            .cacheStore(new DummyInMemoryCacheStore())
                  .fetchPersistentState(true);
   }

   public void testFetchingPersistentStateUpdatesIndex() throws Exception {
      loadCacheEntries(this.<String, Person>caches().get(0));

      // Before adding a node, verify that the query resolves properly
      Cache<String, Person> cache1 = this.<String, Person>caches().get(0);
      executeSimpleQuery(cache1);

      // Since passivation is enabled, cache stores should still be empty
      checkCacheStoresEmpty();

      // Evict manually entries from both nodes
      for (Cache<Object, Object> cache : caches()) {
         for (Person p2 : persons) {
            cache.evict(p2.getName());
         }
      }

      // After eviction, cache stores should be loaded with instances
      checkCacheStoresContainPersons();

      // Finally add a node and verify that state transfer happens and query works
      addNodeCheckingContentsAndQuery();
   }

   private void checkCacheStoresContainPersons() throws CacheLoaderException {
      for (Cache<Object, Object> cache : caches()) {
         CacheStore store = TestingUtil.extractComponent(cache, CacheLoaderManager.class).getCacheStore();
         for (int i = 0; i < persons.length; i++)
            assertEquals(persons[i], store.load(persons[i].getName()).getValue());
      }
   }

   private void checkCacheStoresEmpty() throws CacheLoaderException {
      for (Cache<Object, Object> cache : caches()) {
         CacheStore store = TestingUtil.extractComponent(cache, CacheLoaderManager.class).getCacheStore();
         for (Person person : persons) {
            assert !store.containsKey(person.getName());
         }
      }
   }

}
