package org.infinispan.test.integration.persistence.jdbc.stringbased;

import org.infinispan.commons.test.TestResourceTracker;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.distribution.ch.impl.SingleSegmentKeyPartitioner;
import org.infinispan.marshall.TestObjectStreamMarshaller;
import org.infinispan.persistence.jdbc.configuration.JdbcStringBasedStoreConfigurationBuilder;
import org.infinispan.persistence.jdbc.stringbased.JdbcStringBasedStore;
import org.infinispan.persistence.jdbc.stringbased.JdbcStringBasedStoreAltMapperTest;
import org.infinispan.persistence.jdbc.stringbased.PersonKey2StringMapper;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.persistence.support.WaitDelegatingNonBlockingStore;
import org.infinispan.test.TestDataSCI;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.test.integration.persistence.jdbc.util.DatabaseManager;
import org.infinispan.util.PersistenceMockUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class JdbcStringBasedStoreAltMapperIT extends JdbcStringBasedStoreAltMapperTest {

    protected JdbcStringBasedStoreConfigurationBuilder createJdbcConfig(ConfigurationBuilder builder) {
        JdbcStringBasedStoreConfigurationBuilder storeBuilder = builder
                .persistence()
                .addStore(JdbcStringBasedStoreConfigurationBuilder.class)
                .key2StringMapper(PersonKey2StringMapper.class);
        return storeBuilder;
    }

    @Before
    public void createCacheStore() throws PersistenceException {
        TestResourceTracker.setThreadTestName(super.getTestName());
        ConfigurationBuilder builder = TestCacheManagerFactory.getDefaultCacheConfiguration(false);
        JdbcStringBasedStoreConfigurationBuilder storeBuilder = createJdbcConfig(builder);
        DatabaseManager.buildTableManipulation(storeBuilder);
        DatabaseManager.configureUniqueConnectionFactory(storeBuilder);

        JdbcStringBasedStore jdbcStringBasedStore = new JdbcStringBasedStore();
        cacheStore = new WaitDelegatingNonBlockingStore(jdbcStringBasedStore, SingleSegmentKeyPartitioner.getInstance());
        marshaller = new TestObjectStreamMarshaller(TestDataSCI.INSTANCE);
        cacheStore.startAndWait(PersistenceMockUtil.createContext(JdbcStringBasedStoreAltMapperIT.class, storeBuilder.build(), marshaller));
        tableManager = jdbcStringBasedStore.getTableManager();
    }

    @After
    public void clearStore() {
        super.clearStore();
    }


    /**
     * When trying to persist an unsupported object an exception is expected.
     */
    @Test
    public void persistUnsupportedObject() throws Exception {
        super.persistUnsupportedObject();
    }

    @Test
    public void testStoreLoadRemove() throws Exception {
        super.testStoreLoadRemove();
    }

    @Test
    public void testClear() throws Exception {
        super.testClear();
    }

    @Test
    public void testPurgeExpired() throws Exception {
        super.testPurgeExpired();
    }

}
