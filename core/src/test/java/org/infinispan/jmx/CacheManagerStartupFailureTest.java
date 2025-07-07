package org.infinispan.jmx;

import static org.infinispan.test.TestingUtil.getCacheManagerObjectName;
import static org.testng.AssertJUnit.assertFalse;

import java.io.IOException;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import org.infinispan.commons.jmx.MBeanServerLookup;
import org.infinispan.commons.jmx.TestMBeanServerLookup;
import org.infinispan.commons.test.Exceptions;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.configuration.module.MyParserExtension;
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
        MyParserExtension.enqueueGetNamespacesException(new MyException());
        try (EmbeddedCacheManager cacheManager = createCacheManager()) {
            Exceptions.expectRootCause(MyException.class, cacheManager::start);

            ObjectName objectName = getCacheManagerObjectName(JMX_DOMAIN, "DefaultCacheManager");
            assertFalse(server.isRegistered(objectName));
        } finally {
            MyParserExtension.removeGetNamespacesException();
        }
    }

    private EmbeddedCacheManager createCacheManager() {
        GlobalConfigurationBuilder globalConfiguration = new GlobalConfigurationBuilder();
        globalConfiguration.jmx().enabled(true).domain(JMX_DOMAIN).mBeanServerLookup(mBeanServerLookup);
        ConfigurationBuilder configuration = new ConfigurationBuilder();
        return TestCacheManagerFactory.createCacheManager(globalConfiguration, configuration, false);
    }

    private static class MyException extends RuntimeException {
    }
}
