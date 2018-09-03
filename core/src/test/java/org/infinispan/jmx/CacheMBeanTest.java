package org.infinispan.jmx;

import static org.infinispan.test.TestingUtil.checkMBeanOperationParameterNaming;
import static org.infinispan.test.TestingUtil.getCacheManagerObjectName;
import static org.infinispan.test.TestingUtil.getCacheObjectName;
import static org.infinispan.test.TestingUtil.getMethodSpecificJmxDomain;
import static org.infinispan.test.TestingUtil.withCacheManager;
import static org.infinispan.test.fwk.TestCacheManagerFactory.createCacheManagerEnforceJmxDomain;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import java.lang.reflect.Method;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.infinispan.commons.jmx.PerThreadMBeanServerLookup;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.manager.EmbeddedCacheManagerStartupException;
import org.infinispan.test.CacheManagerCallable;
import org.infinispan.test.Exceptions;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.Test;

@Test(groups = {"functional", "smoke"}, testName = "jmx.CacheMBeanTest")
public class CacheMBeanTest extends MultipleCacheManagersTest {
   private static final Log log = LogFactory.getLog(CacheMBeanTest.class);
   public static final String JMX_DOMAIN = CacheMBeanTest.class.getSimpleName();
   private EmbeddedCacheManager cacheManager;
   private MBeanServer server;

   @Override
   protected void createCacheManagers() throws Exception {
      cacheManager = createCacheManagerEnforceJmxDomain(JMX_DOMAIN);
      registerCacheManager(cacheManager);
      // Create the default cache and register its JMX beans
      cacheManager.getCache();
      server = PerThreadMBeanServerLookup.getThreadMBeanServer();
   }

   public void testJmxOperationMetadata() throws Exception {
      ObjectName name = getCacheObjectName(JMX_DOMAIN);
      checkMBeanOperationParameterNaming(name);
   }

   public void testStartStopManagedOperations() throws Exception {
      ObjectName defaultOn = getCacheObjectName(JMX_DOMAIN);
      ObjectName managerON = getCacheManagerObjectName(JMX_DOMAIN);
      server.invoke(managerON, "startCache", new Object[]{}, new String[]{});
      assert ComponentStatus.RUNNING.toString().equals(server.getAttribute(defaultOn, "CacheStatus"));
      assert server.getAttribute(managerON, "CreatedCacheCount").equals("1");
      assert server.getAttribute(managerON, "RunningCacheCount").equals("1");
      server.invoke(defaultOn, "stop", new Object[]{}, new String[]{});
      assert ComponentStatus.TERMINATED.toString().equals(server.getAttribute(defaultOn, "CacheStatus"));
      assert server.getAttribute(managerON, "CreatedCacheCount").equals("1");
      assert server.getAttribute(managerON, "RunningCacheCount").equals("0");
      server.invoke(defaultOn, "start", new Object[]{}, new String[]{});
      assert ComponentStatus.RUNNING.toString().equals(server.getAttribute(defaultOn, "CacheStatus"));
      assert server.getAttribute(managerON, "CreatedCacheCount").equals("1");
      assert server.getAttribute(managerON, "RunningCacheCount").equals("1");
      server.invoke(defaultOn, "stop", new Object[]{}, new String[]{});
      assert server.getAttribute(managerON, "CreatedCacheCount").equals("1");
      assert server.getAttribute(managerON, "RunningCacheCount").equals("0");
      assert ComponentStatus.TERMINATED.toString().equals(server.getAttribute(defaultOn, "CacheStatus"));
      server.invoke(defaultOn, "start", new Object[]{}, new String[]{});
      assert server.getAttribute(managerON, "CreatedCacheCount").equals("1");
      assert server.getAttribute(managerON, "RunningCacheCount").equals("1");
      assert ComponentStatus.RUNNING.toString().equals(server.getAttribute(defaultOn, "CacheStatus"));
      server.invoke(defaultOn, "stop", new Object[]{}, new String[]{});
      assert server.getAttribute(managerON, "CreatedCacheCount").equals("1");
      assert server.getAttribute(managerON, "RunningCacheCount").equals("0");
      assert ComponentStatus.TERMINATED.toString().equals(server.getAttribute(defaultOn, "CacheStatus"));
   }

   public void testManagerStopRemovesCacheMBean(Method m) throws Exception {
      final String otherJmxDomain = getMethodSpecificJmxDomain(m, JMX_DOMAIN);
      ObjectName defaultOn = getCacheObjectName(otherJmxDomain);
      ObjectName galderOn = getCacheObjectName(otherJmxDomain, "galder(local)");
      ObjectName managerON = getCacheManagerObjectName(otherJmxDomain);
      EmbeddedCacheManager otherContainer = createCacheManagerEnforceJmxDomain(otherJmxDomain);
      otherContainer.defineConfiguration("galder", new ConfigurationBuilder().build());
      registerCacheManager(otherContainer);
      server.invoke(managerON, "startCache", new Object[]{}, new String[]{});
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


   public void testDuplicateJmxDomainOnlyCacheExposesJmxStatistics() throws Exception {
      Exceptions.expectException(EmbeddedCacheManagerStartupException.class, JmxDomainConflictException.class,
                                 () -> createCacheManagerEnforceJmxDomain(JMX_DOMAIN, false, true, false));
   }

   public void testAvoidLeakOfCacheMBeanWhenCacheStatisticsDisabled(Method m) {
      final String jmxDomain = "jmx_" + m.getName();
      withCacheManager(new CacheManagerCallable(
            createCacheManagerEnforceJmxDomain(jmxDomain, false, false, true)) {
         @Override
         public void call() throws Exception {
            cm.getCache();
            ObjectName cacheObjectName = getCacheObjectName(jmxDomain);
            assertTrue(cacheObjectName + " should be registered",
                  server.isRegistered(cacheObjectName));
            TestingUtil.killCacheManagers(cm);
            assertFalse(cacheObjectName + " should NOT be registered",
                  server.isRegistered(cacheObjectName));
         }
      });
   }

}
