package org.infinispan.persistence.jdbc.stringbased;

import org.infinispan.persistence.jdbc.configuration.JdbcStringBasedStoreConfigurationBuilder;
import org.infinispan.persistence.jdbc.common.impl.connectionfactory.SimpleConnectionFactory;
import org.infinispan.persistence.jdbc.common.connectionfactory.ConnectionFactory;
import org.testng.annotations.Test;

import static org.infinispan.persistence.jdbc.UnitTestDatabaseManager.configureSimpleConnectionFactory;

@Test(groups = "functional", testName = "persistence.jdbc.stringbased.StringStoreWithManagedConnectionFunctionalTest")
public class StringStoreWithManagedConnectionFunctionalTest extends AbstractStringBasedCacheStore {

    @Override
    protected ConnectionFactory getConnectionFactory(JdbcStringBasedStoreConfigurationBuilder storeBuilder) {
        SimpleConnectionFactory simpleFactory = new SimpleConnectionFactory();
        simpleFactory.start(configureSimpleConnectionFactory(storeBuilder).create(), Thread.currentThread().getContextClassLoader());
        return simpleFactory;
    }
}
