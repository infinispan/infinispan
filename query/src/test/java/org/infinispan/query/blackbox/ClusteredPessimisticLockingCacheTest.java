package org.infinispan.query.blackbox;

import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.query.helper.SearchConfig;
import org.infinispan.query.helper.StaticTestingErrorHandler;
import org.infinispan.query.test.CustomKey3;
import org.infinispan.query.test.CustomKey3Transformer;
import org.infinispan.query.test.Person;
import org.infinispan.query.test.QueryTestSCI;
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
            .enable()
            .storage(LOCAL_HEAP)
            .addIndexedEntity(Person.class)
            .addKeyTransformer(CustomKey3.class, CustomKey3Transformer.class)
            .addProperty(SearchConfig.ERROR_HANDLER, StaticTestingErrorHandler.class.getName());
      enhanceConfig(cacheCfg);
      createClusteredCaches(2, QueryTestSCI.INSTANCE, cacheCfg);
      cache1 = cache(0);
      cache2 = cache(1);
   }

   protected boolean transactionsEnabled() {
      return true;
   }
}
