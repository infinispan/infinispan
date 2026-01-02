package org.infinispan.jmx;

import static org.infinispan.test.TestingUtil.getCacheManagerObjectName;
import static org.testng.AssertJUnit.assertFalse;

import java.io.IOException;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.infinispan.commands.module.TestGlobalConfigurationBuilder;
import org.infinispan.commons.jmx.MBeanServerLookup;
import org.infinispan.commons.jmx.TestMBeanServerLookup;
import org.infinispan.commons.test.Exceptions;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.AbstractCacheTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * @author Réda Housni Alaoui
 */
@Test(groups = "functional", testName = "jmx.CacheManagerStartupFailureTest")
public class CacheManagerStartupFailureTest extends AbstractCacheTest {

    private static final String JMX_DOMAIN = CacheManagerStartupFailureTest.class.getSimpleName();
    private final MBeanServerLookup mBeanServerLookup = TestMBeanServerLookup.create();

    private final MBeanServer server = mBeanServerLookup.getMBeanServer();

    public void testUnregisterOnCacheManagerStartupFailure() throws IOException {
        try (EmbeddedCacheManager cacheManager = createCacheManager()) {
            Exceptions.expectRootCause(MyException.class, cacheManager::start);

            ObjectName objectName = getCacheManagerObjectName(JMX_DOMAIN, "DefaultCacheManager");
            assertFalse(server.isRegistered(objectName));
        }
    }

    private EmbeddedCacheManager createCacheManager() {
        FailingGlobalComponent failingGlobalComponent = new FailingGlobalComponent();
        GlobalConfigurationBuilder globalConfiguration = new GlobalConfigurationBuilder();
        globalConfiguration.addModule(TestGlobalConfigurationBuilder.class)
                .testGlobalComponent(FailingGlobalComponent.class.getSimpleName(), failingGlobalComponent);
        globalConfiguration.jmx().enabled(true).domain(JMX_DOMAIN).mBeanServerLookup(mBeanServerLookup);
        ConfigurationBuilder configuration = new ConfigurationBuilder();
        return TestCacheManagerFactory.createCacheManager(globalConfiguration, configuration, false);
    }

    private static class MyException extends RuntimeException {
    }

    @Scope(Scopes.GLOBAL)
    public static class FailingGlobalComponent {

        @Start
        public void start() {
            throw new MyException();
        }
    }
}
