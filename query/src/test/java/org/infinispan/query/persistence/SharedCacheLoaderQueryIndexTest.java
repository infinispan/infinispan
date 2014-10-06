package org.infinispan.query.persistence;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.persistence.dummy.DummyInMemoryStore;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.persistence.spi.CacheLoader;
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

   public void testPreloadIndexingAfterAddingNewNode() throws Exception {
      loadCacheEntries(this.<String, Person>caches().get(0));

      for (CacheLoader cs: TestingUtil.cachestores(this.<String, Person>caches())) {
         assert cs.contains(persons[0].getName()) :
               "Cache misconfigured, maybe cache store not pointing to same place, maybe passivation on...etc";
         DummyInMemoryStore dimcs = (DummyInMemoryStore) cs;
         assert dimcs.stats().get("clear") == 0:
               "Cache store should not be cleared, purgeOnStartup is false";
         assert dimcs.stats().get("write") == 4:
               "Cache store should have been written to 4 times, but was written to " + dimcs.stats().get("write") + " times";
      }

      // Before adding a node, verify that the query resolves properly
      executeSimpleQuery(this.<String, Person>caches().get(0));

      addNodeCheckingContentsAndQuery();
   }

}
