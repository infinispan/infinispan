package org.infinispan.jmx;

import static org.infinispan.test.TestingUtil.checkMBeanOperationParameterNaming;
import static org.infinispan.test.TestingUtil.getCacheManagerObjectName;
import static org.infinispan.test.TestingUtil.getCacheObjectName;
import static org.infinispan.test.TestingUtil.withCacheManager;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import java.lang.reflect.Method;

import javax.management.InstanceNotFoundException;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.infinispan.commons.jmx.MBeanServerLookup;
import org.infinispan.commons.jmx.MBeanServerLookupProvider;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.manager.EmbeddedCacheManagerStartupException;
import org.infinispan.test.CacheManagerCallable;
import org.infinispan.test.Exceptions;
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
   private EmbeddedCacheManager cacheManager;
   private final MBeanServerLookup mBeanServerLookup = MBeanServerLookupProvider.create();

   @Override
   protected void createCacheManagers() throws Exception {
      GlobalConfigurationBuilder globalConfiguration = new GlobalConfigurationBuilder();
      globalConfiguration
            .cacheContainer().statistics(true)
            .globalJmxStatistics()
            .allowDuplicateDomains(true)
            .jmxDomain(JMX_DOMAIN)
            .mBeanServerLookup(mBeanServerLookup);
      ConfigurationBuilder configuration = new ConfigurationBuilder();
      configuration.jmxStatistics().enabled(false);
      cacheManager = TestCacheManagerFactory.createCacheManager(globalConfiguration, configuration, true);

      registerCacheManager(cacheManager);
      // Create the default cache and register its JMX beans
      cacheManager.getCache();
   }

   public void testJmxOperationMetadata() throws Exception {
      ObjectName name = getCacheObjectName(JMX_DOMAIN, getDefaultCacheName() + "(local)");
      checkMBeanOperationParameterNaming(mBeanServerLookup.getMBeanServer(), name);
   }

   public void testStartStopManagedOperations() throws Exception {
      ObjectName defaultOn = getCacheObjectName(JMX_DOMAIN, getDefaultCacheName() + "(local)");
      ObjectName managerON = getCacheManagerObjectName(JMX_DOMAIN);
      MBeanServer server = mBeanServerLookup.getMBeanServer();
      server.invoke(managerON, "startCache", new Object[0], new String[0]);
      assert ComponentStatus.RUNNING.toString().equals(server.getAttribute(defaultOn, "CacheStatus"));
      assert server.getAttribute(managerON, "CreatedCacheCount").equals("1");
      assert server.getAttribute(managerON, "RunningCacheCount").equals("1");
      server.invoke(defaultOn, "stop", new Object[0], new String[0]);
      assert ComponentStatus.TERMINATED.toString().equals(server.getAttribute(defaultOn, "CacheStatus"));
      assert server.getAttribute(managerON, "CreatedCacheCount").equals("1");
      assert server.getAttribute(managerON, "RunningCacheCount").equals("0");
      server.invoke(defaultOn, "start", new Object[0], new String[0]);
      assert ComponentStatus.RUNNING.toString().equals(server.getAttribute(defaultOn, "CacheStatus"));
      assert server.getAttribute(managerON, "CreatedCacheCount").equals("1");
      assert server.getAttribute(managerON, "RunningCacheCount").equals("1");
      server.invoke(defaultOn, "stop", new Object[0], new String[0]);
      assert server.getAttribute(managerON, "CreatedCacheCount").equals("1");
      assert server.getAttribute(managerON, "RunningCacheCount").equals("0");
      assert ComponentStatus.TERMINATED.toString().equals(server.getAttribute(defaultOn, "CacheStatus"));
      server.invoke(defaultOn, "start", new Object[0], new String[0]);
      assert server.getAttribute(managerON, "CreatedCacheCount").equals("1");
      assert server.getAttribute(managerON, "RunningCacheCount").equals("1");
      assert ComponentStatus.RUNNING.toString().equals(server.getAttribute(defaultOn, "CacheStatus"));
      server.invoke(defaultOn, "stop", new Object[0], new String[0]);
      assert server.getAttribute(managerON, "CreatedCacheCount").equals("1");
      assert server.getAttribute(managerON, "RunningCacheCount").equals("0");
      assert ComponentStatus.TERMINATED.toString().equals(server.getAttribute(defaultOn, "CacheStatus"));
   }

   public void testManagerStopRemovesCacheMBean(Method m) throws Exception {
      String otherJmxDomain = JMX_DOMAIN + "_" + m.getName();
      ObjectName defaultOn = getCacheObjectName(otherJmxDomain, getDefaultCacheName() + "(local)");
      ObjectName galderOn = getCacheObjectName(otherJmxDomain, "galder(local)");
      ObjectName managerON = getCacheManagerObjectName(otherJmxDomain);
      GlobalConfigurationBuilder gc = new GlobalConfigurationBuilder();
      gc.cacheContainer().statistics(true)
            .globalJmxStatistics()
            .allowDuplicateDomains(true)
            .jmxDomain(otherJmxDomain)
            .mBeanServerLookup(mBeanServerLookup);
      ConfigurationBuilder c = new ConfigurationBuilder();
      c.jmxStatistics().enabled(true);
      EmbeddedCacheManager otherContainer = TestCacheManagerFactory.createCacheManager(gc, c, true);
      otherContainer.defineConfiguration("galder", new ConfigurationBuilder().build());
      registerCacheManager(otherContainer);
      MBeanServer server = mBeanServerLookup.getMBeanServer();
      server.invoke(managerON, "startCache", new Object[0], new String[0]);
      server.invoke(managerON, "startCache", new Object[]{"galder"}, new String[]{String.class.getName()});
      assert ComponentStatus.RUNNING.toString().equals(server.getAttribute(defaultOn, "CacheStatus"));
      assert ComponentStatus.RUNNING.toString().equals(server.getAttribute(galderOn, "CacheStatus"));
      otherContainer.stop();
      try {
         log.info(server.getMBeanInfo(managerON));
         assert false : "Failure expected, " + managerON + " shouldn't be registered in mbean server";
      } catch (InstanceNotFoundException e) {
      }
      try {
         log.info(server.getMBeanInfo(defaultOn));
         assert false : "Failure expected, " + defaultOn + " shouldn't be registered in mbean server";
      } catch (InstanceNotFoundException e) {
      }
      try {
         log.info(server.getMBeanInfo(galderOn));
         assert false : "Failure expected, " + galderOn + " shouldn't be registered in mbean server";
      } catch (InstanceNotFoundException e) {
      }
   }

   public void testDuplicateJmxDomainOnlyCacheExposesJmxStatistics() {
      GlobalConfigurationBuilder gc = new GlobalConfigurationBuilder();
      gc.cacheContainer().statistics(false)
            .globalJmxStatistics()
            .allowDuplicateDomains(false)
            .jmxDomain(JMX_DOMAIN)
            .mBeanServerLookup(mBeanServerLookup);
      ConfigurationBuilder c = new ConfigurationBuilder();
      c.jmxStatistics().enabled(true);

      Exceptions.expectException(EmbeddedCacheManagerStartupException.class, JmxDomainConflictException.class,
            () -> TestCacheManagerFactory.createCacheManager(gc, c, true));
   }

   public void testAvoidLeakOfCacheMBeanWhenCacheStatisticsDisabled(Method m) {
      String otherJmxDomain = "jmx_" + m.getName();
      GlobalConfigurationBuilder gc = new GlobalConfigurationBuilder();
      gc.cacheContainer().statistics(false)
            .globalJmxStatistics()
            .allowDuplicateDomains(true)
            .jmxDomain(otherJmxDomain)
            .mBeanServerLookup(mBeanServerLookup);
      ConfigurationBuilder c = new ConfigurationBuilder();
      c.jmxStatistics().available(false);

      withCacheManager(new CacheManagerCallable(TestCacheManagerFactory.createCacheManager(gc, c, true)) {
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
