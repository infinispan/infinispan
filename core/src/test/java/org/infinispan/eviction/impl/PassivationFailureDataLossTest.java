package org.infinispan.eviction.impl;

import static org.infinispan.test.TestingUtil.getFirstStore;
import static org.testng.AssertJUnit.assertNotNull;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.support.FailStore;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * Reproducer for data loss when passivation fails
 */
@Test(groups = "functional", testName = "eviction.impl.PassivationFailureDataLossTest")
public class PassivationFailureDataLossTest extends SingleCacheManagerTest {

    @Override
    protected EmbeddedCacheManager createCacheManager() throws Exception {
        ConfigurationBuilder cfg = getDefaultStandaloneCacheConfig(false);
        cfg.memory().maxCount(2);
        cfg.persistence()
           .passivation(true)
           .addStore(FailStore.ConfigurationBuilder.class);
        return TestCacheManagerFactory.createCacheManager(new GlobalConfigurationBuilder().nonClusteredDefault(), cfg);
    }

    public void testEntryNotLostWhenPassivationFails() {
        cache.put("k1", "v1");
        cache.put("k2", "v2");

        FailStore fs = getFirstStore(cache);
        fs.failModification(1);  // next passivation write throws TestException

        // maxCount will trigger the passivation
        cache.put("k3", "v3");

        eventually(() -> cache.getAdvancedCache().getDataContainer().size() <= 2);

        assertNotNull("cannot recover k1 entry", cache.get("k1"));
        assertNotNull("cannot recover k2 entry", cache.get("k2"));
        assertNotNull("cannot recover k2 entry", cache.get("k3"));
    }
}
