package org.infinispan.test.integration.persistence.jdbc;

import org.infinispan.persistence.jdbc.PooledConnectionFactoryTest;
import org.infinispan.persistence.jdbc.common.configuration.PooledConnectionFactoryConfiguration;
import org.infinispan.persistence.spi.PersistenceException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.infinispan.test.integration.persistence.jdbc.util.DatabaseManager.configureUniqueConnectionFactory;
import static org.infinispan.test.integration.persistence.jdbc.util.DatabaseManager.createConfigurationBuilder;

public class PooledConnectionFactoryIT {

    private PooledConnectionFactoryTest testClass;

    public PooledConnectionFactoryConfiguration connectionFactoryConfiguration() {
        return configureUniqueConnectionFactory(createConfigurationBuilder());
    }

    @Before
    public void beforeMethod() {
        testClass.beforeMethod();
    }

    @After
    public void destroyFactory() {
        testClass.destroyFactory();
    }

    public PooledConnectionFactoryIT() {
        testClass = new PooledConnectionFactoryTest(connectionFactoryConfiguration());
    }

    @Test(expected = PersistenceException.class)
    public void testNoDriverClassFound() {
        testClass.testNoDriverClassFound();
    }

}
