package org.infinispan.test.integration.persistence.jdbc.stringbased;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.persistence.jdbc.common.configuration.ConnectionFactoryConfiguration;
import org.infinispan.persistence.jdbc.common.configuration.ManagedConnectionFactoryConfiguration;
import org.infinispan.persistence.jdbc.common.configuration.ManagedConnectionFactoryConfigurationBuilder;
import org.infinispan.persistence.jdbc.common.connectionfactory.ConnectionFactory;
import org.infinispan.persistence.jdbc.common.impl.connectionfactory.ManagedConnectionFactory;
import org.infinispan.persistence.jdbc.configuration.JdbcStringBasedStoreConfigurationBuilder;
import org.infinispan.persistence.jdbc.stringbased.StringStoreWithManagedConnectionFunctionalTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.test.integration.persistence.jdbc.util.DatabaseManager;
import org.junit.Test;

import java.util.Properties;

public class StringBasedCacheStoreCFManagedIT {

    private StringStoreWithManagedConnectionFunctionalTest testClass;

    @Test
    public void testPutGetRemoveWithoutPassivationWithPreload() throws Exception {
        this.testClass.testPutGetRemoveWithoutPassivationWithPreload();
    }

    @Test
    public void testPutGetRemoveWithPassivationWithoutPreload() throws Exception {
        testClass.testPutGetRemoveWithPassivationWithoutPreload();
    }

    public StringBasedCacheStoreCFManagedIT() {
        testClass = new StringStoreWithManagedConnectionFunctionalTest() {
            @Override
            protected ConnectionFactory getConnectionFactory(JdbcStringBasedStoreConfigurationBuilder storeBuilder) {
                ConnectionFactoryConfiguration connectionFactoryConfiguration = storeBuilder
                        .dataSource()
                        .read(connectionFactoryConfiguration())
                        .create();
                final ConnectionFactory connectionFactory = ConnectionFactory.getConnectionFactory(ManagedConnectionFactory.class);
                connectionFactory.start(connectionFactoryConfiguration, connectionFactory.getClass().getClassLoader());
                return connectionFactory;
            }

            @Override
            public void setTableManipulation(JdbcStringBasedStoreConfigurationBuilder storeBuilder) {
                DatabaseManager.buildTableManipulation(storeBuilder);
            }
        };
    }

    public ManagedConnectionFactoryConfiguration connectionFactoryConfiguration() {
        Properties props = DatabaseManager.loadDBProperties();
        ConfigurationBuilder builder = TestCacheManagerFactory.getDefaultCacheConfiguration(false);
        final ManagedConnectionFactoryConfigurationBuilder<JdbcStringBasedStoreConfigurationBuilder> managedConnectionFactoryConfigurationBuilder = builder
                .persistence().addStore(JdbcStringBasedStoreConfigurationBuilder.class)
                .dataSource();
//        managedConnectionFactoryConfigurationBuilder.jndiUrl(props.getProperty("datasource.jndi.location"));
        managedConnectionFactoryConfigurationBuilder.jndiUrl(props.getProperty("java:jboss/datasources/ExampleDS"));

        return managedConnectionFactoryConfigurationBuilder.create();
    }

}
