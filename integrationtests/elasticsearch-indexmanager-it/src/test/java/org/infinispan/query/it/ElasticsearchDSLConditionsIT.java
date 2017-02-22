package org.infinispan.query.it;

import org.infinispan.query.dsl.embedded.ClusteredQueryDslConditionsTest;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * @since 9.0
 */
@Test(groups = "functional", testName = "query.dsl.embedded.ElasticsearchDSLConditionsIT")
public class ElasticsearchDSLConditionsIT extends ClusteredQueryDslConditionsTest {

    @Override
    // HSEARCH-2389
    protected boolean testNullCollections() {
        return false;
    }

    @Override
    protected Map<String, String> getIndexConfig() {
        Map<String, String> indexConfig = new HashMap<>();
        indexConfig.put("default.indexmanager", "elasticsearch");
        indexConfig.put("default.elasticsearch.refresh_after_write", "true");
        indexConfig.put("lucene_version", "LUCENE_CURRENT");
        return indexConfig;
    }
}
