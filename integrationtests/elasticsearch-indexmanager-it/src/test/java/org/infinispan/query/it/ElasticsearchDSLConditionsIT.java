package org.infinispan.query.it;

import org.infinispan.query.dsl.embedded.ClusteredQueryDslConditionsTest;
import org.testng.annotations.Test;

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
        return ElasticsearchTesting.getConfigProperties();
    }

}
