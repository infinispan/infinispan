package org.infinispan.jmx;

import static org.infinispan.test.TestingUtil.checkMBeanOperationParameterNaming;
import static org.infinispan.test.TestingUtil.getCacheManagerObjectName;
import static org.infinispan.test.TestingUtil.getCacheObjectName;
import static org.infinispan.test.TestingUtil.withCacheManager;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;

import java.lang.reflect.Method;

import javax.management.InstanceNotFoundException;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.infinispan.commons.jmx.MBeanServerLookup;
import org.infinispan.commons.jmx.TestMBeanServerLookup;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.manager.EmbeddedCacheManagerStartupException;
import org.infinispan.test.CacheManagerCallable;
import org.infinispan.commons.test.Exceptions;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.Test;

@Test(groups = {"functional", "smoke"}, testName = "jmx.CacheMBeanTest")
public class CacheMBeanTest extends MultipleCacheManagersTest {

   private static final Log log = LogFactory.getLog(CacheMBeanTest.class);

   public static final String JMX_DOMAIN = CacheMBeanTest.class.getSimpleName();

   private final MBeanServerLookup mBeanServerLookup = TestMBeanServerLookup.create();

   @Override
   protected void createCacheManagers() throws Exception {
      GlobalConfigurationBuilder globalConfiguration = new GlobalConfigurationBuilder();
      globalConfiguration
            .jmx().enabled(true)
            .domain(JMX_DOMAIN)
            .mBeanServerLookup(mBeanServerLookup);
      ConfigurationBuilder configuration = new ConfigurationBuilder();
      EmbeddedCacheManager cacheManager = TestCacheManagerFactory.createCacheManager(globalConfiguration, configuration);

      registerCacheManager(cacheManager);
      // Create the default cache and register its JMX beans
      cacheManager.getCache();
   }

   public void testJmxOperationMetadata() throws Exception {
      ObjectName name = getCacheObjectName(JMX_DOMAIN, getDefaultCacheName() + "(local)");
      checkMBeanOperationParameterNaming(mBeanServerLookup.getMBeanServer(), name);
   }

   public void testStartStopManagedOperations() throws Exception {
      ObjectName cacheObjectName = getCacheObjectName(JMX_DOMAIN, getDefaultCacheName() + "(local)");
      ObjectName cacheManagerObjectName = getCacheManagerObjectName(JMX_DOMAIN);
      MBeanServer server = mBeanServerLookup.getMBeanServer();
      server.invoke(cacheManagerObjectName, "startCache", new Object[0], new String[0]);
      assertEquals(ComponentStatus.RUNNING.toString(), server.getAttribute(cacheObjectName, "CacheStatus"));
      assertEquals("1", server.getAttribute(cacheManagerObjectName, "CreatedCacheCount"));
      assertEquals("1", server.getAttribute(cacheManagerObjectName, "RunningCacheCount"));
      server.invoke(cacheObjectName, "stop", new Object[0], new String[0]);
      assertFalse(cacheObjectName + " should NOT be registered", server.isRegistered(cacheObjectName));
      assertEquals("1", server.getAttribute(cacheManagerObjectName, "CreatedCacheCount"));
      assertEquals("0", server.getAttribute(cacheManagerObjectName, "RunningCacheCount"));
      server.invoke(cacheManagerObjectName, "startCache", new Object[]{getDefaultCacheName()}, new String[]{String.class.getName()});
      assertEquals(ComponentStatus.RUNNING.toString(), server.getAttribute(cacheObjectName, "CacheStatus"));
      assertEquals("1", server.getAttribute(cacheManagerObjectName, "CreatedCacheCount"));
      assertEquals("1", server.getAttribute(cacheManagerObjectName, "RunningCacheCount"));
      server.invoke(cacheObjectName, "stop", new Object[0], new String[0]);
      assertFalse(cacheObjectName + " should NOT be registered", server.isRegistered(cacheObjectName));
      assertEquals("1", server.getAttribute(cacheManagerObjectName, "CreatedCacheCount"));
      assertEquals("0", server.getAttribute(cacheManagerObjectName, "RunningCacheCount"));
      server.invoke(cacheManagerObjectName, "startCache", new Object[]{getDefaultCacheName()}, new String[]{String.class.getName()});
      assertEquals(ComponentStatus.RUNNING.toString(), server.getAttribute(cacheObjectName, "CacheStatus"));
      assertEquals("1", server.getAttribute(cacheManagerObjectName, "CreatedCacheCount"));
      assertEquals("1", server.getAttribute(cacheManagerObjectName, "RunningCacheCount"));
      server.invoke(cacheObjectName, "stop", new Object[0], new String[0]);
      assertFalse(cacheObjectName + " should NOT be registered", server.isRegistered(cacheObjectName));
      assertEquals("1", server.getAttribute(cacheManagerObjectName, "CreatedCacheCount"));
      assertEquals("0", server.getAttribute(cacheManagerObjectName, "RunningCacheCount"));
   }

   public void testManagerStopRemovesCacheMBean(Method m) throws Exception {
      String otherJmxDomain = JMX_DOMAIN + "_" + m.getName();
      ObjectName defaultOn = getCacheObjectName(otherJmxDomain, getDefaultCacheName() + "(local)");
      ObjectName galderOn = getCacheObjectName(otherJmxDomain, "galder(local)");
      ObjectName managerON = getCacheManagerObjectName(otherJmxDomain);
      GlobalConfigurationBuilder gc = new GlobalConfigurationBuilder();
      gc.jmx().enabled(true)
        .domain(otherJmxDomain)
        .mBeanServerLookup(mBeanServerLookup);
      ConfigurationBuilder c = new ConfigurationBuilder();
      c.statistics().enabled(true);
      EmbeddedCacheManager otherContainer = TestCacheManagerFactory.createCacheManager(gc, c);
      otherContainer.defineConfiguration("galder", new ConfigurationBuilder().build());
      registerCacheManager(otherContainer);
      MBeanServer server = mBeanServerLookup.getMBeanServer();
      server.invoke(managerON, "startCache", new Object[0], new String[0]);
      server.invoke(managerON, "startCache", new Object[]{"galder"}, new String[]{String.class.getName()});
      assertEquals(ComponentStatus.RUNNING.toString(), server.getAttribute(defaultOn, "CacheStatus"));
      assertEquals(ComponentStatus.RUNNING.toString(), server.getAttribute(galderOn, "CacheStatus"));
      otherContainer.stop();
      try {
         log.info(server.getMBeanInfo(managerON));
         fail("Failure expected, " + managerON + " shouldn't be registered in mbean server");
      } catch (InstanceNotFoundException e) {
      }
      try {
         log.info(server.getMBeanInfo(defaultOn));
         fail("Failure expected, " + defaultOn + " shouldn't be registered in mbean server");
      } catch (InstanceNotFoundException e) {
      }
      try {
         log.info(server.getMBeanInfo(galderOn));
         fail("Failure expected, " + galderOn + " shouldn't be registered in mbean server");
      } catch (InstanceNotFoundException e) {
      }
   }

   public void testDuplicateJmxDomainOnlyCacheExposesJmxStatistics() {
      GlobalConfigurationBuilder gc = new GlobalConfigurationBuilder();
      gc.jmx().enabled(true)
        .domain(JMX_DOMAIN)
        .mBeanServerLookup(mBeanServerLookup);
      ConfigurationBuilder c = new ConfigurationBuilder();
      c.statistics().enabled(true);

      Exceptions.expectException(EmbeddedCacheManagerStartupException.class, JmxDomainConflictException.class,
            () -> TestCacheManagerFactory.createCacheManager(gc, c));
   }

   public void testAvoidLeakOfCacheMBeanWhenCacheStatisticsDisabled(Method m) {
      String otherJmxDomain = "jmx_" + m.getName();
      GlobalConfigurationBuilder gc = new GlobalConfigurationBuilder();
      gc.jmx().enabled(true)
        .domain(otherJmxDomain)
        .mBeanServerLookup(mBeanServerLookup);
      ConfigurationBuilder c = new ConfigurationBuilder();
      c.statistics().available(false);

      withCacheManager(new CacheManagerCallable(TestCacheManagerFactory.createCacheManager(gc, c)) {
         @Override
         public void call() {
            cm.getCache();
            MBeanServer server = mBeanServerLookup.getMBeanServer();
            ObjectName cacheObjectName = getCacheObjectName(otherJmxDomain, getDefaultCacheName() + "(local)");
            assertTrue(cacheObjectName + " should be registered", server.isRegistered(cacheObjectName));
            TestingUtil.killCacheManagers(cm);
            assertFalse(cacheObjectName + " should NOT be registered", server.isRegistered(cacheObjectName));
         }
      });
   }
}
