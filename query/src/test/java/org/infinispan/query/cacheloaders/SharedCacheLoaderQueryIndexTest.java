package org.infinispan.query.cacheloaders;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.loaders.dummy.DummyInMemoryCacheStore;
import org.infinispan.loaders.dummy.DummyInMemoryCacheStoreConfigurationBuilder;
import org.infinispan.loaders.spi.CacheStore;
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

   @Override
   protected void configureCache(ConfigurationBuilder builder) {
      // To force a shared cache store, make sure storeName property
      // for dummy store is the same for all nodes
      builder.clustering().stateTransfer().fetchInMemoryState(false)
         .loaders().shared(true).preload(true).addStore(DummyInMemoryCacheStoreConfigurationBuilder.class).
            storeName(getClass().getName());
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
