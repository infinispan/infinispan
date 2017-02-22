package org.infinispan.query.it;


import org.hibernate.search.elasticsearch.impl.ElasticsearchIndexManager;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.api.AnotherTestEntity;
import org.infinispan.query.api.NonIndexedValuesTest;
import org.infinispan.query.api.TestEntity;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/*
 * @since 9.0
 */
@Test(groups = "functional", testName = "query.api.ElasticSearchNonIndexedValuesIT")
public class ElasticSearchNonIndexedValuesIT extends NonIndexedValuesTest {

    @Override
    protected EmbeddedCacheManager createCacheManager() throws Exception {
        ConfigurationBuilder c = getDefaultStandaloneCacheConfig(isTransactional());
        c.indexing()
                .index(Index.LOCAL)
                .addIndexedEntity(TestEntity.class)
                .addIndexedEntity(AnotherTestEntity.class)
                .addProperty("default.indexmanager", ElasticsearchIndexManager.class.getName())
                .addProperty("default.elasticsearch.refresh_after_write", "true")
                .addProperty("error_handler", "org.infinispan.query.helper.StaticTestingErrorHandler")
                .addProperty("lucene_version", "LUCENE_CURRENT");
        return TestCacheManagerFactory.createCacheManager(c);
    }

    protected boolean isTransactional() {
        return false;
    }
}
