package org.infinispan.query.blackbox;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.infinispan.hibernate.search.spi.InfinispanIntegration;
import org.infinispan.query.test.Person;
import org.infinispan.query.test.QueryTestSCI;
import org.testng.annotations.Test;

/**
 * Tests verifying Querying on REPL cache configured with InfinispanIndexManager and infinispan directory provider.
 *
 * @author Anna Manukyan
 */
@Test(groups = "functional", testName = "query.blackbox.ClusteredCacheWithInfinispanDirectoryTest")
public class ClusteredCacheWithInfinispanDirectoryTest extends ClusteredCacheTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      createCluster(holder -> {
         String defaultName = "default";
         holder.getGlobalConfigurationBuilder().defaultCacheName(defaultName).serialization().addContextInitializer(QueryTestSCI.INSTANCE);

         ConfigurationBuilder cacheCfg = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, transactionsEnabled());
         cacheCfg.indexing()
               .index(Index.PRIMARY_OWNER)
               .addIndexedEntity(Person.class)
               .addProperty("default.indexmanager", "org.infinispan.query.indexmanager.InfinispanIndexManager")
               .addProperty("error_handler", "org.infinispan.query.helper.StaticTestingErrorHandler")
               .addProperty("lucene_version", "LUCENE_CURRENT");
         cacheCfg.clustering().stateTransfer().fetchInMemoryState(true);
         enhanceConfig(cacheCfg);
         holder.newConfigurationBuilder(defaultName).read(cacheCfg.build());

         Configuration cacheCfg1 =
               getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, false)
                     .clustering().stateTransfer().fetchInMemoryState(true).build();
         holder.newConfigurationBuilder(InfinispanIntegration.DEFAULT_INDEXESDATA_CACHENAME).read(cacheCfg1);
         holder.newConfigurationBuilder(InfinispanIntegration.DEFAULT_LOCKING_CACHENAME).read(cacheCfg1);
      }, 2);

      cache1 = cache(0);
      cache2 = cache(1);
   }

}
