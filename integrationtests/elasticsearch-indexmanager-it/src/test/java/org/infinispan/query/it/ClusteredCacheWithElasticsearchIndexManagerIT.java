package org.infinispan.query.it;

import java.util.List;

import org.hibernate.search.elasticsearch.impl.ElasticsearchIndexManager;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.infinispan.distribution.ch.impl.AffinityPartitioner;
import org.infinispan.query.blackbox.ClusteredCacheTest;
import org.infinispan.query.test.Person;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

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
    public void testSearchKeyTransformer() throws Exception {
        // Will be fixed in Hibernate Search v. 5.8.0.Beta2 : see HSEARCH-2688
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
        List<Cache<Object, Person>> caches = createClusteredCaches(2, cacheCfg);
        cache1 = caches.get(0);
        cache2 = caches.get(1);
    }

    @AfterMethod
    @Override
    protected void clearContent() throws Throwable {
        // super.clearContent() clears the data container and the stores of all the non-private caches.
        // Invoke clear() instead to clear the indexes stored in elasticsearch.
        cache(0).clear();
    }
}
