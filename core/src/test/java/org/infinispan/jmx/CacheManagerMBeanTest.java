package org.infinispan.jmx;

import static java.lang.String.format;
import static org.infinispan.factories.KnownComponentNames.TIMEOUT_SCHEDULE_EXECUTOR;
import static org.infinispan.test.TestingUtil.checkMBeanOperationParameterNaming;
import static org.infinispan.test.TestingUtil.extractGlobalComponent;
import static org.infinispan.test.TestingUtil.getCacheManagerObjectName;
import static org.infinispan.test.TestingUtil.getCacheObjectName;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertTrue;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.concurrent.ScheduledExecutorService;

import javax.management.InstanceNotFoundException;
import javax.management.MBeanException;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.ServiceNotFoundException;

import org.infinispan.commons.jmx.MBeanServerLookup;
import org.infinispan.commons.jmx.TestMBeanServerLookup;
import org.infinispan.commons.test.Exceptions;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.CacheContainer;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.manager.EmbeddedCacheManagerStartupException;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * Tests whether the attributes defined by DefaultCacheManager work correct.
 *
 * @author Mircea.Markus@jboss.com
 * @author Galder Zamarreño
 * @since 4.0
 */
@Test(groups = "functional", testName = "jmx.CacheManagerMBeanTest")
public class CacheManagerMBeanTest extends SingleCacheManagerTest {

   private static final String JMX_DOMAIN = CacheManagerMBeanTest.class.getSimpleName();

   private final MBeanServerLookup mBeanServerLookup = TestMBeanServerLookup.create();

   private final MBeanServer server = mBeanServerLookup.getMBeanServer();

   private ObjectName name;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      GlobalConfigurationBuilder globalConfiguration = new GlobalConfigurationBuilder();
      globalConfiguration.jmx().enabled(true).domain(JMX_DOMAIN).mBeanServerLookup(mBeanServerLookup);
      ConfigurationBuilder configuration = new ConfigurationBuilder();
      cacheManager = TestCacheManagerFactory.createCacheManager(globalConfiguration, configuration);
      name = getCacheManagerObjectName(JMX_DOMAIN);
      mBeanServerLookup.getMBeanServer().invoke(name, "startCache", new Object[0], new String[0]);
      return cacheManager;
   }

   public void testJmxOperations() throws Exception {
      assertEquals("1", server.getAttribute(name, "CreatedCacheCount"));
      assertEquals("1", server.getAttribute(name, "DefinedCacheCount"));
      assertEquals(format("[%s(created)]", getDefaultCacheName()), server.getAttribute(name, "DefinedCacheNames"));
      assertEquals("1", server.getAttribute(name, "RunningCacheCount"));

      //now define some new caches
      cacheManager.defineConfiguration("a", new ConfigurationBuilder().build());
      cacheManager.defineConfiguration("b", new ConfigurationBuilder().build());
      cacheManager.defineConfiguration("c", new ConfigurationBuilder().build());
      assertEquals("1", server.getAttribute(name, "CreatedCacheCount"));
      assertEquals("4", server.getAttribute(name, "DefinedCacheCount"));
      assertEquals("1", server.getAttribute(name, "RunningCacheCount"));
      String attribute = (String) server.getAttribute(name, "DefinedCacheConfigurationNames");
      String[] names = attribute.substring(1, attribute.length() - 1).split(",");
      assertTrue(Arrays.binarySearch(names, "a") >= 0);
      assertTrue(Arrays.binarySearch(names, "b") >= 0);
      assertTrue(Arrays.binarySearch(names, "c") >= 0);

      //now start some caches
      server.invoke(name, "startCache", new Object[]{"a"}, new String[]{String.class.getName()});
      server.invoke(name, "startCache", new Object[]{"b"}, new String[]{String.class.getName()});
      assertEquals("3", server.getAttribute(name, "CreatedCacheCount"));
      assertEquals("4", server.getAttribute(name, "DefinedCacheCount"));
      assertEquals("3", server.getAttribute(name, "RunningCacheCount"));
      attribute = (String) server.getAttribute(name, "DefinedCacheNames");
      assertTrue(attribute.contains("a("));
      assertTrue(attribute.contains("b("));
      assertTrue(attribute.contains("c("));
   }

   public void testJmxOperationMetadata() throws Exception {
      checkMBeanOperationParameterNaming(mBeanServerLookup.getMBeanServer(), name);
   }

   public void testInvokeJmxOperationNotExposed() {
      Exceptions.expectException(MBeanException.class, ServiceNotFoundException.class,
            () -> mBeanServerLookup.getMBeanServer().invoke(name, "stop", Util.EMPTY_OBJECT_ARRAY, Util.EMPTY_STRING_ARRAY));
   }

   public void testSameDomain() {
      GlobalConfigurationBuilder gc = new GlobalConfigurationBuilder();
      gc.jmx().enabled(true).domain(JMX_DOMAIN).mBeanServerLookup(mBeanServerLookup);
      ConfigurationBuilder c = new ConfigurationBuilder();

      Exceptions.expectException(EmbeddedCacheManagerStartupException.class, () -> TestCacheManagerFactory.createCacheManager(gc, c));
   }

   public void testJmxRegistrationAtStartupAndStop(Method m) throws Exception {
      String otherJmxDomain = JMX_DOMAIN + "_" + m.getName();
      GlobalConfigurationBuilder gc = new GlobalConfigurationBuilder();
      gc.jmx().enabled(true).domain(otherJmxDomain).mBeanServerLookup(mBeanServerLookup);
      CacheContainer otherContainer = TestCacheManagerFactory.createCacheManager(gc, null);
      ObjectName otherName = getCacheManagerObjectName(otherJmxDomain);
      try {
         assertEquals("0", mBeanServerLookup.getMBeanServer().getAttribute(otherName, "CreatedCacheCount"));
      } finally {
         otherContainer.stop();
      }

      Exceptions.expectException(InstanceNotFoundException.class, () -> mBeanServerLookup.getMBeanServer().getAttribute(otherName, "CreatedCacheCount"));
   }

   public void testCustomCacheManagerName(Method m) throws Exception {
      String otherJmxDomain = JMX_DOMAIN + "_" + m.getName();
      GlobalConfigurationBuilder gc = new GlobalConfigurationBuilder();
      gc.jmx().enabled(true).domain(otherJmxDomain).mBeanServerLookup(mBeanServerLookup);
      gc.cacheManagerName("Hibernate2LC");
      CacheContainer otherContainer = TestCacheManagerFactory.createCacheManager(gc, null);
      try {
         ObjectName otherName = getCacheManagerObjectName(otherJmxDomain, "Hibernate2LC");
         assertEquals("0", mBeanServerLookup.getMBeanServer().getAttribute(otherName, "CreatedCacheCount"));
      } finally {
         otherContainer.stop();
      }
   }

   public void testAddressInformation() throws Exception {
      assertEquals("local", server.getAttribute(name, "NodeAddress"));
      assertEquals("local", server.getAttribute(name, "ClusterMembers"));
      assertEquals("local", server.getAttribute(name, "PhysicalAddresses"));
      assertEquals(1, server.getAttribute(name, "ClusterSize"));
   }

   @Test(dependsOnMethods = "testJmxOperations")
   public void testCacheMBeanUnregisterOnRemove() {
      cacheManager.defineConfiguration("test", new ConfigurationBuilder().build());
      assertNotNull(cacheManager.getCache("test"));
      ObjectName cacheMBean = getCacheObjectName(JMX_DOMAIN, "test(local)");
      assertTrue(server.isRegistered(cacheMBean));
      cacheManager.administration().removeCache("test");
      assertFalse(server.isRegistered(cacheMBean));
   }

   public void testExecutorMBeans() throws Exception {
      ScheduledExecutorService timeoutExecutor =
            extractGlobalComponent(cacheManager, ScheduledExecutorService.class, TIMEOUT_SCHEDULE_EXECUTOR);
      timeoutExecutor.submit(() -> {});

      ObjectName objectName = getCacheManagerObjectName(JMX_DOMAIN, "DefaultCacheManager", TIMEOUT_SCHEDULE_EXECUTOR);
      assertTrue(server.isRegistered(objectName));
      assertEquals(1, server.getAttribute(objectName, "PoolSize"));
      assertEquals(Integer.MAX_VALUE, server.getAttribute(objectName, "MaximumPoolSize"));
   }
}
