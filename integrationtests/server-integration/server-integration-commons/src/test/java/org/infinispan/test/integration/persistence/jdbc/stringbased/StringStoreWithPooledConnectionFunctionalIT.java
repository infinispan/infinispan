package org.infinispan.test.integration.persistence.jdbc.stringbased;

import org.infinispan.Cache;
import org.infinispan.commons.io.ByteBufferFactory;
import org.infinispan.commons.test.TestResourceTracker;
import org.infinispan.commons.time.TimeService;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.StoreConfiguration;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.eviction.EvictionType;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.marshall.TestObjectStreamMarshaller;
import org.infinispan.persistence.DummyInitializationContext;
import org.infinispan.persistence.jdbc.common.configuration.PooledConnectionFactoryConfiguration;
import org.infinispan.persistence.jdbc.configuration.JdbcStringBasedStoreConfigurationBuilder;
import org.infinispan.persistence.jdbc.impl.table.TableManagerFactory;
import org.infinispan.persistence.jdbc.stringbased.StringStoreWithPooledConnectionFunctionalTest;
import org.infinispan.persistence.spi.InitializationContext;
import org.infinispan.persistence.spi.MarshallableEntryFactory;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.test.integration.persistence.jdbc.util.DatabaseManager;
import org.infinispan.util.concurrent.BlockingManager;
import org.infinispan.util.concurrent.NonBlockingManager;
import org.junit.Test;

import java.util.concurrent.ExecutorService;

import static org.infinispan.test.integration.persistence.jdbc.util.DatabaseManager.configureUniqueConnectionFactory;
import static org.infinispan.test.integration.persistence.jdbc.util.DatabaseManager.createConfigurationBuilder;

public class StringStoreWithPooledConnectionFunctionalIT extends StringStoreWithPooledConnectionFunctionalTest {

    public StringStoreWithPooledConnectionFunctionalIT() {
        super(connectionFactoryConfiguration());
        TestResourceTracker.setThreadTestName(super.getTestName());
    }

    public static PooledConnectionFactoryConfiguration connectionFactoryConfiguration() {
        return configureUniqueConnectionFactory(createConfigurationBuilder());
    }

    @Override
    public EmbeddedCacheManager configureCacheManager(boolean passivation, boolean preload, boolean eviction) {
        GlobalConfigurationBuilder gcb = new GlobalConfigurationBuilder().nonClusteredDefault().defaultCacheName("StringBasedCache");
        ConfigurationBuilder builder = TestCacheManagerFactory.getDefaultCacheConfiguration(false);
        builder. persistence().passivation(false);
        JdbcStringBasedStoreConfigurationBuilder storeBuilder = builder
                .persistence()
                .passivation(passivation)
                .addStore(JdbcStringBasedStoreConfigurationBuilder.class)
                .preload(preload);

        connectionFactory = getConnectionFactory(storeBuilder);
        DatabaseManager.buildTableManipulation(storeBuilder);

        if (eviction) {
            builder.memory().evictionType(EvictionType.COUNT).size(2L);
        } else {
            builder.memory().evictionType(EvictionType.COUNT).size(-1L);
        }

        tableConfiguration = storeBuilder.create().table();
        EmbeddedCacheManager defaultCacheManager = TestCacheManagerFactory.newDefaultCacheManager(true, gcb, builder);
        String cacheName = defaultCacheManager.getCache().getName();

        InitializationContext ctx = new DummyInitializationContext((StoreConfiguration)null, (Cache)null, new TestObjectStreamMarshaller(), (ByteBufferFactory)null, (MarshallableEntryFactory)null, (ExecutorService)null, (GlobalConfiguration)null, (BlockingManager)null, (NonBlockingManager)null, (TimeService)null);
        tableManager = TableManagerFactory.getManager(ctx, connectionFactory, storeBuilder.create(), cacheName);
        return defaultCacheManager;
    }

    @Test
    public void testPutGetRemoveWithoutPassivationWithPreload() throws Exception {
        super.testPutGetRemoveWithoutPassivationWithPreload();
    }

    @Test
    public void testPutGetRemoveWithPassivationWithoutPreload() throws Exception {
        super.testPutGetRemoveWithPassivationWithoutPreload();
    }

}
