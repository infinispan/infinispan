package org.infinispan.query.it;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.distribution.ch.impl.AffinityPartitioner;
import org.infinispan.query.blackbox.ClusteredCacheTest;
import org.infinispan.query.test.Person;
import org.infinispan.query.test.QueryTestSCI;
import org.testng.annotations.Test;

/**
 * @since 9.0
 */
@Test(groups = "functional", testName = "query.blackbox.ClusteredCacheWithElasticsearchIndexManagerIT")
public class ClusteredCacheWithElasticsearchIndexManagerIT extends ClusteredCacheTest {

    @Override
    public void testCombinationOfFilters() {
        // Not supported by hibernate search
    }

    @Override
    public void testFullTextFilterOnOff() {
        // Not supported by hibernate search
    }

    @Override
    public void testSearchKeyTransformer() {
        // Will be fixed in Hibernate Search v. 5.8.0.Beta2 : see HSEARCH-2688
    }

    @Override
    protected void createCacheManagers() {
        ConfigurationBuilder cacheCfg = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, transactionsEnabled());
        cacheCfg.clustering().hash().keyPartitioner(new AffinityPartitioner());
        cacheCfg.indexing()
                .enable()
                .addIndexedEntity(Person.class);
        ElasticsearchTesting.applyTestProperties(cacheCfg.indexing());
        createClusteredCaches(2, QueryTestSCI.INSTANCE, cacheCfg);
        cache1 = cache(0);
        cache2 = cache(1);
    }
}
