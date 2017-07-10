package org.infinispan.query.blackbox;

import java.util.List;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.infinispan.query.test.Person;
import org.infinispan.transaction.LockingMode;
import org.testng.annotations.Test;

/**
 * Tests which test the querying on Clustered cache with Pessimistic Lock for transaction.
 *
 * @author Anna Manukyan
 */
@Test(groups = "functional", testName = "query.blackbox.ClusteredPessimisticLockingCacheTest")
public class ClusteredPessimisticLockingCacheTest extends ClusteredCacheTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder cacheCfg = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, transactionsEnabled());
      cacheCfg.transaction().lockingMode(LockingMode.PESSIMISTIC);
      cacheCfg.indexing()
            .index(Index.ALL)
            .addIndexedEntity(Person.class)
            .addProperty("default.directory_provider", "local-heap")
            .addProperty("error_handler", "org.infinispan.query.helper.StaticTestingErrorHandler")
            .addProperty("lucene_version", "LUCENE_CURRENT");
      enhanceConfig(cacheCfg);
      List<Cache<Object, Person>> caches = createClusteredCaches(2, cacheCfg);
      cache1 = caches.get(0);
      cache2 = caches.get(1);
   }

   protected boolean transactionsEnabled() {
      return true;
   }
}
