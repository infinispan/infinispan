package org.infinispan.query.it;

import org.hibernate.search.elasticsearch.impl.ElasticsearchIndexManager;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.infinispan.distribution.ch.impl.AffinityPartitioner;
import org.infinispan.query.blackbox.ClusteredCacheTest;
import org.infinispan.query.test.Person;
import org.testng.annotations.Test;

import java.util.List;

/**
 * @since 9.0
 */
@Test(groups = "functional", testName = "query.blackbox.ClusteredCacheWithElasticsearchIndexManagerIT")
public class ClusteredCacheWithElasticsearchIndexManagerIT extends ClusteredCacheTest {


    @Override
    public void testCombinationOfFilters() throws Exception {
        // Not supported by hibernate search
    }

    @Override
    public void testFullTextFilterOnOff() throws Exception {
        // Not supported by hibernate search
    }

    @Override
    protected void createCacheManagers() throws Throwable {
        ConfigurationBuilder cacheCfg = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, transactionsEnabled());
        cacheCfg.clustering().hash().keyPartitioner(new AffinityPartitioner());
        cacheCfg.indexing()
                .index(Index.LOCAL)
                .addIndexedEntity(Person.class)
                .addProperty("default.indexmanager", ElasticsearchIndexManager.class.getName())
                .addProperty("default.elasticsearch.refresh_after_write", "true")
                .addProperty("error_handler", "org.infinispan.query.helper.StaticTestingErrorHandler");
        List<Cache<String, Person>> caches = createClusteredCaches(2, cacheCfg);
        cache1 = caches.get(0);
        cache2 = caches.get(1);
    }

}
