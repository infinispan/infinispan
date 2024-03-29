package org.infinispan.persistence.jdbc.stringbased;

import static org.infinispan.persistence.jdbc.UnitTestDatabaseManager.configureUniqueConnectionFactory;

import org.infinispan.commons.configuration.Combine;
import org.infinispan.persistence.jdbc.common.configuration.ConnectionFactoryConfiguration;
import org.infinispan.persistence.jdbc.common.configuration.PooledConnectionFactoryConfiguration;
import org.infinispan.persistence.jdbc.common.connectionfactory.ConnectionFactory;
import org.infinispan.persistence.jdbc.common.impl.connectionfactory.PooledConnectionFactory;
import org.infinispan.persistence.jdbc.configuration.JdbcStringBasedStoreConfigurationBuilder;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "persistence.jdbc.stringbased.StringStoreWithPooledConnectionFunctionalTest")
public class StringStoreWithPooledConnectionFunctionalTest extends AbstractStringBasedCacheStore {
    private PooledConnectionFactoryConfiguration customFactoryConfiguration;

    public StringStoreWithPooledConnectionFunctionalTest() {}

    //Invoked from external resource
    @SuppressWarnings("unused")
    public StringStoreWithPooledConnectionFunctionalTest(PooledConnectionFactoryConfiguration customFactoryConfiguration) {
        this.customFactoryConfiguration = customFactoryConfiguration;
    }

    @Override
    public ConnectionFactory getConnectionFactory(JdbcStringBasedStoreConfigurationBuilder storeBuilder) {
        ConnectionFactoryConfiguration connectionFactoryConfiguration;
        if(customFactoryConfiguration != null) {
            connectionFactoryConfiguration = storeBuilder.connectionPool().read(customFactoryConfiguration, Combine.DEFAULT).create();
        } else {
            connectionFactoryConfiguration = configureUniqueConnectionFactory(storeBuilder).create();
        }

        final ConnectionFactory connectionFactory = ConnectionFactory.getConnectionFactory(PooledConnectionFactory.class);
        connectionFactory.start(connectionFactoryConfiguration, connectionFactory.getClass().getClassLoader());
        return connectionFactory;
    }
}
