package org.infinispan.jmx;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.assertFalse;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.regex.Pattern;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.CacheContainer;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import static org.infinispan.test.TestingUtil.*;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;


import javax.management.InstanceNotFoundException;
import javax.management.MBeanException;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.ServiceNotFoundException;

/**
 * Tests whether the attributes defined by DefaultCacheManager work correct.
 *
 * @author Mircea.Markus@jboss.com
 * @author Galder Zamarreño
 * @since 4.0
 */
@Test(groups = "functional", testName = "jmx.CacheManagerMBeanTest")
public class CacheManagerMBeanTest extends SingleCacheManagerTest {

   public static final String JMX_DOMAIN = CacheManagerMBeanTest.class.getSimpleName();

   private MBeanServer server;
   private ObjectName name;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      cacheManager = TestCacheManagerFactory.createCacheManagerEnforceJmxDomain(JMX_DOMAIN, true, false);
      name = getCacheManagerObjectName(JMX_DOMAIN);
      server = PerThreadMBeanServerLookup.getThreadMBeanServer();
      server.invoke(name, "startCache", new Object[]{}, new String[]{});
      return cacheManager;
   }

   public void testJmxOperations() throws Exception {
      assert server.getAttribute(name, "CreatedCacheCount").equals("1");
      assert server.getAttribute(name, "DefinedCacheCount").equals("0") : "Was " + server.getAttribute(name, "DefinedCacheCount");
      assert server.getAttribute(name, "DefinedCacheNames").equals("[]");
      assert server.getAttribute(name, "RunningCacheCount").equals("1");

      //now define some new caches
      cacheManager.defineConfiguration("a", new ConfigurationBuilder().build());
      cacheManager.defineConfiguration("b", new ConfigurationBuilder().build());
      cacheManager.defineConfiguration("c", new ConfigurationBuilder().build());
      assert server.getAttribute(name, "CreatedCacheCount").equals("1");
      assert server.getAttribute(name, "DefinedCacheCount").equals("3");
      assert server.getAttribute(name, "RunningCacheCount").equals("1");
      String attribute = (String) server.getAttribute(name, "DefinedCacheConfigurationNames");
      String names[] = attribute.substring(1, attribute.length()-1).split(",");
      assertTrue(Arrays.binarySearch(names, "a") >= 0);
      assertTrue(Arrays.binarySearch(names, "b") >= 0);
      assertTrue(Arrays.binarySearch(names, "c") >= 0);

      //now start some caches
      server.invoke(name, "startCache", new Object[]{"a"}, new String[]{String.class.getName()});
      server.invoke(name, "startCache", new Object[]{"b"}, new String[]{String.class.getName()});
      assertEquals("3", server.getAttribute(name, "CreatedCacheCount"));
      assertEquals("3", server.getAttribute(name, "DefinedCacheCount"));
      assertEquals("3", server.getAttribute(name, "RunningCacheCount"));
      attribute = (String) server.getAttribute(name, "DefinedCacheNames");
      assertTrue(attribute.contains("a("));
      assertTrue(attribute.contains("b("));
      assertTrue(attribute.contains("c("));
   }

   public void testJmxOperationMetadata() throws Exception {
      checkMBeanOperationParameterNaming(name);
   }

   public void testInvokeJmxOperationNotExposed() throws Exception {
      try {
         server.invoke(name, "stop", new Object[]{}, new String[]{});
         assert false : "Method not exposed, invocation should have failed";
      } catch (MBeanException mbe) {
         assert mbe.getCause() instanceof ServiceNotFoundException;
      }

   }

   public void testJmxRegistrationAtStartupAndStop(Method m) throws Exception {
      final String otherJmxDomain = getMethodSpecificJmxDomain(m, JMX_DOMAIN);
      CacheContainer otherContainer = TestCacheManagerFactory.createCacheManagerEnforceJmxDomain(otherJmxDomain, true, false);
      ObjectName otherName = getCacheManagerObjectName(otherJmxDomain);
      try {
         assert server.getAttribute(otherName, "CreatedCacheCount").equals("0");
      } finally {
         otherContainer.stop();
      }

      try {
         server.getAttribute(otherName, "CreatedCacheCount").equals("0");
         assert false : "Failure expected, " + otherName + " shouldn't be registered in mbean server";
      } catch (InstanceNotFoundException e) {
      }
   }

   public void testCustomCacheManagerName(Method m) throws Exception {
      final String otherJmxDomain = getMethodSpecificJmxDomain(m, JMX_DOMAIN);
      CacheContainer otherContainer = TestCacheManagerFactory.createCacheManagerEnforceJmxDomain(otherJmxDomain, "Hibernate2LC", true, false);
      ObjectName otherName = getCacheManagerObjectName(otherJmxDomain, "Hibernate2LC");
      try {
         assert server.getAttribute(otherName, "CreatedCacheCount").equals("0");
      } finally {
         otherContainer.stop();
      }
   }

   public void testAddressInformation() throws Exception {
      assert server.getAttribute(name, "NodeAddress").equals("local");
      assert server.getAttribute(name, "ClusterMembers").equals("local");
      assert server.getAttribute(name, "PhysicalAddresses").equals("local");
      assert server.getAttribute(name, "ClusterSize").equals(1);
   }

   @Test(dependsOnMethods="testJmxOperations")
   public void testCacheMBeanUnregisterOnRemove() throws Exception {
      Cache<Object, Object> testCache = cacheManager.getCache("test");
      ObjectName cacheMBean = getCacheObjectName(JMX_DOMAIN, "test(local)");
      assertTrue(existsObject(cacheMBean));
      cacheManager.removeCache("test");
      assertFalse(existsObject(cacheMBean));
      cacheManager.getCache("test");
   }

}
