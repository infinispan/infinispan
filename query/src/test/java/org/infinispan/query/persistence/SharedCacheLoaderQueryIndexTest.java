package org.infinispan.query.persistence;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.persistence.dummy.DummyInMemoryStore;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
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
@Test(groups = "functional", testName = "query.persistence.SharedCacheLoaderQueryIndexTest")
public class SharedCacheLoaderQueryIndexTest extends BaseReIndexingTest {

   @Override
   protected void configureCache(ConfigurationBuilder builder) {
      // To force a shared cache store, make sure storeName property
      // for dummy store is the same for all nodes
      builder.clustering().stateTransfer().fetchInMemoryState(false)
            .persistence().addStore(DummyInMemoryStoreConfigurationBuilder.class).shared(true).preload(true).
            storeName(getClass().getName());
   }

   public void testPreloadIndexingAfterAddingNewNode() {
      loadCacheEntries(this.<String, Person>caches().get(0));

      List<DummyInMemoryStore<String, Person>> cacheStores = TestingUtil.cachestores(caches());
      for (DummyInMemoryStore<String, Person> dimcs : cacheStores) {
         assertTrue(dimcs.contains(persons[0].getName()), "Cache misconfigured, maybe cache store not pointing to same place, maybe passivation on...etc");

         int clear = dimcs.stats().get("clear");
         assertEquals(0, clear, "Cache store should not be cleared, purgeOnStartup is false");

         int write = dimcs.stats().get("write");
         assertEquals(4, write, "Cache store should have been written to 4 times, but was written to " + write + " times");
      }

      // Before adding a node, verify that the query resolves properly
      executeSimpleQuery(this.<String, Person>caches().get(0));

      addNodeCheckingContentsAndQuery();
   }
}
