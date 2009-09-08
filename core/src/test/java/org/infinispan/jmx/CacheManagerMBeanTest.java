package org.infinispan.jmx;

import org.infinispan.config.Configuration;
import org.infinispan.config.GlobalConfiguration;
import org.infinispan.manager.CacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import javax.management.MBeanServer;
import javax.management.ObjectName;

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
      GlobalConfiguration globalConfiguration = GlobalConfiguration.getNonClusteredDefault();
      globalConfiguration.setJmxDomain(JMX_DOMAIN);
      globalConfiguration.setMBeanServerLookup(PerThreadMBeanServerLookup.class.getName());
      globalConfiguration.setExposeGlobalJmxStatistics(true);
      cacheManager = TestCacheManagerFactory.createCacheManager(globalConfiguration);
      cacheManager.start();
      cacheManager.getCache();
      name = new ObjectName("CacheManagerMBeanTest:cache-name=[global],jmx-resource=CacheManager");
      server = PerThreadMBeanServerLookup.getThreadMBeanServer();
      return cacheManager;
   }

   public void testJmxOperations() throws Exception {
      assert server.getAttribute(name, "CreatedCacheCount").equals("1");
      assert server.getAttribute(name, "DefinedCacheCount").equals("0") : "Was " + server.getAttribute(name, "DefinedCacheCount");
      assert server.getAttribute(name, "DefinedCacheNames").equals("[]");

      //now define some new caches
      cacheManager.defineConfiguration("a", new Configuration());
      cacheManager.defineConfiguration("b", new Configuration());
      cacheManager.defineConfiguration("c", new Configuration());
      assert server.getAttribute(name, "CreatedCacheCount").equals("1");
      assert server.getAttribute(name, "DefinedCacheCount").equals("3");
      String attribute = (String) server.getAttribute(name, "DefinedCacheNames");
      assert attribute.contains("a(");
      assert attribute.contains("b(");
      assert attribute.contains("c(");

      //now start some caches
      cacheManager.getCache("a");
      cacheManager.getCache("b");
      assert server.getAttribute(name, "CreatedCacheCount").equals("3");
      assert server.getAttribute(name, "DefinedCacheCount").equals("3");
      attribute = (String) server.getAttribute(name, "DefinedCacheNames");
      assert attribute.contains("a(");
      assert attribute.contains("b(");
      assert attribute.contains("c(");
   }
}
