package org.infinispan.persistence.jdbc.stringbased;

import org.infinispan.persistence.jdbc.configuration.ConnectionFactoryConfiguration;
import org.infinispan.persistence.jdbc.configuration.JdbcStringBasedStoreConfigurationBuilder;
import org.infinispan.persistence.jdbc.configuration.ManagedConnectionFactoryConfiguration;
import org.infinispan.persistence.jdbc.impl.connectionfactory.ManagedConnectionFactory;
import org.infinispan.persistence.jdbc.impl.connectionfactory.SimpleConnectionFactory;
import org.infinispan.persistence.jdbc.connectionfactory.ConnectionFactory;
import org.testng.annotations.Test;

import static org.infinispan.persistence.jdbc.UnitTestDatabaseManager.configureSimpleConnectionFactory;

@Test(groups = "functional", testName = "persistence.jdbc.stringbased.StringStoreWithManagedConnectionFunctionalTest")
public class StringStoreWithManagedConnectionFunctionalTest extends AbstractStringBasedCacheStore {

    private ManagedConnectionFactoryConfiguration customFactoryConfiguration;

    public StringStoreWithManagedConnectionFunctionalTest() {}

    @SuppressWarnings("unused")
    public StringStoreWithManagedConnectionFunctionalTest(ManagedConnectionFactoryConfiguration customFactoryConfiguration) {
        this.customFactoryConfiguration = customFactoryConfiguration;
    }

    @Override
    protected ConnectionFactory getConnectionFactory(JdbcStringBasedStoreConfigurationBuilder storeBuilder) {
        if(customFactoryConfiguration != null) {
            ConnectionFactoryConfiguration connectionFactoryConfiguration = storeBuilder
                    .dataSource()
                    .read(customFactoryConfiguration)
                    .create();
            final ConnectionFactory connectionFactory = ConnectionFactory.getConnectionFactory(ManagedConnectionFactory.class);
            connectionFactory.start(connectionFactoryConfiguration, connectionFactory.getClass().getClassLoader());
            return connectionFactory;
        } else {
            SimpleConnectionFactory simpleFactory = new SimpleConnectionFactory();
            simpleFactory.start(configureSimpleConnectionFactory(storeBuilder).create(), Thread.currentThread().getContextClassLoader());
            return simpleFactory;
        }

    }
}
