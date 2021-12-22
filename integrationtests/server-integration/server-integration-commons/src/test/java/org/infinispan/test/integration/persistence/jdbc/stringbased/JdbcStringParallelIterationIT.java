package org.infinispan.test.integration.persistence.jdbc.stringbased;

import org.infinispan.commons.test.TestResourceTracker;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.persistence.jdbc.configuration.JdbcStringBasedStoreConfigurationBuilder;
import org.infinispan.persistence.jdbc.stringbased.JdbcStringParallelIterationTest;
import org.infinispan.test.integration.persistence.jdbc.util.DatabaseManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class JdbcStringParallelIterationIT extends JdbcStringParallelIterationTest {

    @Before
    public void xpto() throws Exception {
        TestResourceTracker.setThreadTestNameIfMissing(getTestName());
        setup();
    }

    @After
    public void afterMethod() {
        super.cleanupAfterMethod();
    }

    @After
    public void destroyAfterMethod() {
        if (cleanupAfterMethod()) teardown();
    }

    @After
    public void clearContent() {
        if (cleanupAfterTest()) clearCacheManager();
    }


    @Override
    protected void configurePersistence(ConfigurationBuilder cb) {
        JdbcStringBasedStoreConfigurationBuilder storeBuilder =
                cb.persistence().addStore(JdbcStringBasedStoreConfigurationBuilder.class);

        DatabaseManager.buildTableManipulation(storeBuilder);
        DatabaseManager.configureUniqueConnectionFactory(storeBuilder);
    }

    @Test
    public void testParallelIterationWithValue() {
        super.testParallelIterationWithValue();
    }

    @Test
    public void testSequentialIterationWithValue() {
        super.testSequentialIterationWithValue();
    }

    @Test
    public void testParallelIterationWithoutValue() {
        super.testParallelIterationWithoutValue();
    }

    @Test
    public void testSequentialIterationWithoutValue() {
        super.testSequentialIterationWithoutValue();
    }

}
