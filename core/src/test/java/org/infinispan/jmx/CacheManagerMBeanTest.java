package org.infinispan.jmx;

import java.lang.reflect.Method;

import org.infinispan.config.Configuration;
import org.infinispan.manager.CacheManager;
import org.infinispan.test.SingleCacheManagerTest;
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
 * @since 4.0
 */
@Test(groups = "functional", testName = "jmx.CacheManagerMBeanTest")
public class CacheManagerMBeanTest extends SingleCacheManagerTest {

   public static final String JMX_DOMAIN = CacheManagerMBeanTest.class.getSimpleName();

   private MBeanServer server;
   private ObjectName name;

   protected CacheManager createCacheManager() throws Exception {
      cacheManager = TestCacheManagerFactory.createJmxEnabledCacheManager(JMX_DOMAIN, true, false);
      name = new ObjectName(JMX_DOMAIN + ":cache-name=[global],jmx-resource=CacheManager");
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
      cacheManager.defineConfiguration("a", new Configuration());
      cacheManager.defineConfiguration("b", new Configuration());
      cacheManager.defineConfiguration("c", new Configuration());
      assert server.getAttribute(name, "CreatedCacheCount").equals("1");
      assert server.getAttribute(name, "DefinedCacheCount").equals("3");
      assert server.getAttribute(name, "RunningCacheCount").equals("1");
      String attribute = (String) server.getAttribute(name, "DefinedCacheNames");
      assert attribute.contains("a(");
      assert attribute.contains("b(");
      assert attribute.contains("c(");

      //now start some caches
      server.invoke(name, "startCache", new Object[]{"a"}, new String[]{String.class.getName()});
      server.invoke(name, "startCache", new Object[]{"b"}, new String[]{String.class.getName()});
      assert server.getAttribute(name, "CreatedCacheCount").equals("3");
      assert server.getAttribute(name, "DefinedCacheCount").equals("3");
      assert server.getAttribute(name, "RunningCacheCount").equals("3");
      attribute = (String) server.getAttribute(name, "DefinedCacheNames");
      assert attribute.contains("a(");
      assert attribute.contains("b(");
      assert attribute.contains("c(");
   }
   
   public void testInvokeJmxOperationNotExposed() throws Exception {
      try {
         server.invoke(name, "stop", new Object[]{}, new String[]{});
         assert false : "Method not exposed, invocation should have failed";
      } catch (MBeanException mbe) {
         assert mbe.getCause() instanceof ServiceNotFoundException;
      }
      
   }
   
   public void testJmxRegistrationAtStartupAndStop(Method method) throws Exception {
      final String otherJmxDomain = JMX_DOMAIN + '.' + method.getName();
      CacheManager otherManager = TestCacheManagerFactory.createJmxEnabledCacheManager(otherJmxDomain, true, false);
      ObjectName otherName = new ObjectName(otherJmxDomain + ":cache-name=[global],jmx-resource=CacheManager");
      try {
         assert server.getAttribute(otherName, "CreatedCacheCount").equals("0");
      } finally {
         otherManager.stop();
      }
      
      try {
         server.getAttribute(otherName, "CreatedCacheCount").equals("0");
         assert false : "Failure expected, " + otherName + " shouldn't be registered in mbean server";
      } catch (InstanceNotFoundException e) {
      }
   }
}
