package org.infinispan.jmx;

import org.infinispan.config.Configuration;
import org.infinispan.config.GlobalConfiguration;
import org.infinispan.manager.CacheContainer;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * Functional test for checking jmx statistics exposure.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
@Test(groups = "functional", testName = "jmx.JmxStatsFunctionalTest")
public class JmxStatsFunctionalTest extends AbstractInfinispanTest {

   public static final String JMX_DOMAIN = JmxStatsFunctionalTest.class.getSimpleName();
   private MBeanServer server;
   private EmbeddedCacheManager cm, cm2, cm3;


   @AfterMethod(alwaysRun = true)
   public void destroyCacheManager() {
      TestingUtil.killCacheManagers(cm, cm2, cm3);
      cm = null;
      cm2 = null;
      cm3 = null;
      server = null;
      if (cm != null)
         assert !existsDomains(cm.getGlobalConfiguration().getJmxDomain());
      if (cm2 != null)
         assert !existsDomains(cm2.getGlobalConfiguration().getJmxDomain());
      if (cm3 != null)
         assert !existsDomains(cm3.getGlobalConfiguration().getJmxDomain());
   }

   /**
    * Create a local cache, two replicated caches and see that everithing is correctly registered.
    */
   public void testDefaultDomain() {
      GlobalConfiguration globalConfiguration = GlobalConfiguration.getClusteredDefault();
      globalConfiguration.setExposeGlobalJmxStatistics(true);
      globalConfiguration.setMBeanServerLookup(PerThreadMBeanServerLookup.class.getName());
      cm = TestCacheManagerFactory.createCacheManager(globalConfiguration);
      String jmxDomain = cm.getGlobalConfiguration().getJmxDomain();

      Configuration localCache = config();//local by default
      cm.defineConfiguration("local_cache", localCache);
      Configuration remote1 = config();//local by default
      remote1.setCacheMode(Configuration.CacheMode.REPL_SYNC);
      cm.defineConfiguration("remote1", remote1);
      Configuration remote2 = config();//local by default
      remote2.setCacheMode(Configuration.CacheMode.INVALIDATION_ASYNC);
      cm.defineConfiguration("remote2", remote2);

      cm.getCache("local_cache");
      cm.getCache("remote1");
      cm.getCache("remote2");

      assert existsObject(jmxDomain + ":cache-name=\"local_cache(local)\",jmx-resource=Statistics");
      assert existsObject(jmxDomain + ":cache-name=\"remote1(repl_sync)\",jmx-resource=RpcManager");
      assert existsObject(jmxDomain + ":cache-name=\"remote1(repl_sync)\",jmx-resource=Statistics");
      assert existsObject(jmxDomain + ":cache-name=\"remote2(invalidation_async)\",jmx-resource=RpcManager");
      assert existsObject(jmxDomain + ":cache-name=\"remote2(invalidation_async)\",jmx-resource=Statistics");

      TestingUtil.killCacheManagers(cm);

      assert !existsObject(jmxDomain + ":cache-name=\"local_cache(local)\",jmx-resource=Statistics");
      assert !existsObject(jmxDomain + ":cache-name=\"remote1(repl_sync)\",jmx-resource=RpcManager");
      assert !existsObject(jmxDomain + ":cache-name=\"remote1(repl_sync)\",jmx-resource=Statistics");
      assert !existsObject(jmxDomain + ":cache-name=\"remote2(invalidation_async)\",jmx-resource=RpcManager");
      assert !existsObject(jmxDomain + ":cache-name=\"remote2(invalidation_async)\",jmx-resource=Statistics");
   }

   public void testDifferentDomain() {
      GlobalConfiguration globalConfiguration = GlobalConfiguration.getClusteredDefault();
      globalConfiguration.setMBeanServerLookup(PerThreadMBeanServerLookup.class.getName());
      cm = TestCacheManagerFactory.createCacheManager(globalConfiguration);
      String jmxDomain = cm.getGlobalConfiguration().getJmxDomain();

      Configuration localCache = config();//local by default
      cm.defineConfiguration("local_cache", localCache);
      cm.getCache("local_cache");

      assert existsObject(jmxDomain + ":cache-name=\"local_cache(local)\",jmx-resource=Statistics");
   }


   public void testOnlyGlobalJmxStatsEnabled() {
      GlobalConfiguration globalConfiguration = GlobalConfiguration.getClusteredDefault();
      globalConfiguration.setExposeGlobalJmxStatistics(true);
      globalConfiguration.setMBeanServerLookup(PerThreadMBeanServerLookup.class.getName());
      cm = TestCacheManagerFactory.createCacheManager(globalConfiguration);
      String jmxDomain = globalConfiguration.getJmxDomain();

      Configuration localCache = config();//local by default
      localCache.setExposeJmxStatistics(false);
      cm.defineConfiguration("local_cache", localCache);
      Configuration remote1 = config();//local by default
      remote1.setExposeJmxStatistics(false);
      remote1.setCacheMode(Configuration.CacheMode.REPL_SYNC);
      cm.defineConfiguration("remote1", remote1);

      cm.getCache("local_cache");
      cm.getCache("remote1");

      assert !existsObject(jmxDomain + ":cache-name=\"local_cache(local)\",jmx-resource=Statistics");
      assert existsObject(jmxDomain + ":cache-name=\"[global]\",jmx-resource=CacheManager");
      assert !existsObject(jmxDomain + ":cache-name=\"remote1(repl_sync)\",jmx-resource=Statistics");
   }

   public void testOnlyPerCacheJmxStatsEnabled() {
      GlobalConfiguration globalConfiguration = GlobalConfiguration.getClusteredDefault();
      globalConfiguration.setExposeGlobalJmxStatistics(false);
      globalConfiguration.setMBeanServerLookup(PerThreadMBeanServerLookup.class.getName());
      cm = TestCacheManagerFactory.createCacheManager(globalConfiguration);
      String jmxDomain = globalConfiguration.getJmxDomain();

      Configuration localCache = config();//local by default
      localCache.setExposeJmxStatistics(true);
      cm.defineConfiguration("local_cache", localCache);
      Configuration remote1 = config();//local by default
      remote1.setExposeJmxStatistics(true);
      remote1.setCacheMode(Configuration.CacheMode.REPL_SYNC);
      cm.defineConfiguration("remote1", remote1);

      cm.getCache("local_cache");
      cm.getCache("remote1");

      assert existsObject(jmxDomain + ":cache-name=\"local_cache(local)\",jmx-resource=Statistics");
      assert !existsObject(jmxDomain + ":cache-name=\"[global]\",jmx-resource=RpcManager");
      assert existsObject(jmxDomain + ":cache-name=\"remote1(repl_sync)\",jmx-resource=Statistics");
   }

   public void testMultipleManagersOnSameServerFails(Method method) throws Exception {
      final String jmxDomain = JMX_DOMAIN + '.' + method.getName();
      GlobalConfiguration globalConfiguration = GlobalConfiguration.getClusteredDefault();
      globalConfiguration.setJmxDomain(jmxDomain);
      globalConfiguration.setMBeanServerLookup(PerThreadMBeanServerLookup.class.getName());
      cm = TestCacheManagerFactory.createCacheManagerEnforceJmxDomain(globalConfiguration);
      Configuration localCache = config();//local by default
      localCache.setExposeJmxStatistics(true);
      cm.defineConfiguration("local_cache", localCache);
      cm.getCache("local_cache");
      assert existsObject(jmxDomain + ":cache-name=\"local_cache(local)\",jmx-resource=Statistics");

      GlobalConfiguration globalConfiguration2 = GlobalConfiguration.getClusteredDefault();
      globalConfiguration2.setJmxDomain(jmxDomain);
      globalConfiguration2.setExposeGlobalJmxStatistics(true);
      globalConfiguration2.setAllowDuplicateDomains(false);
      globalConfiguration2.setMBeanServerLookup(PerThreadMBeanServerLookup.class.getName());
      try {
         TestCacheManagerFactory.createCacheManagerEnforceJmxDomain(globalConfiguration2);
         assert false : "Failure expected, '" + jmxDomain + "' duplicate!";
      } catch (JmxDomainConflictException e) {
      }

      server = PerThreadMBeanServerLookup.getThreadMBeanServer();
      globalConfiguration2.setAllowDuplicateDomains(true);
      CacheContainer duplicateAllowedContainer = TestCacheManagerFactory.createCacheManagerEnforceJmxDomain(globalConfiguration2);
      try {
         final String duplicateName = jmxDomain + "2";
         ObjectName duplicateObjectName = new ObjectName(duplicateName + ":cache-name=\"[global]\",jmx-resource=CacheManager");
         server.getAttribute(duplicateObjectName, "CreatedCacheCount").equals("0");
      } finally {
         duplicateAllowedContainer.stop();
      }
   }

   public void testMultipleManagersOnSameServerWithCloneFails() {
      GlobalConfiguration globalConfiguration = GlobalConfiguration.getClusteredDefault();
      globalConfiguration.setMBeanServerLookup(PerThreadMBeanServerLookup.class.getName());
      cm = TestCacheManagerFactory.createCacheManager(globalConfiguration);
      Configuration localCache = config();//local by default
      localCache.setExposeJmxStatistics(true);
      cm.defineConfiguration("local_cache", localCache);
      cm.getCache("local_cache");
      String jmxDomain = globalConfiguration.getJmxDomain();
      assert existsObject(jmxDomain + ":cache-name=\"local_cache(local)\",jmx-resource=Statistics");

      GlobalConfiguration globalConfigurationClone = globalConfiguration.clone();
      globalConfigurationClone.setExposeGlobalJmxStatistics(true);
      globalConfigurationClone.setAllowDuplicateDomains(false);
      globalConfigurationClone.setMBeanServerLookup(PerThreadMBeanServerLookup.class.getName());
      try {
         TestCacheManagerFactory.createCacheManagerEnforceJmxDomain(globalConfigurationClone);
         assert false : "Failure expected!";
      } catch (JmxDomainConflictException e) {
      }
   }

   public void testMultipleManagersOnSameServer() {
      GlobalConfiguration globalConfiguration = GlobalConfiguration.getClusteredDefault();
      globalConfiguration.setAllowDuplicateDomains(true);
      globalConfiguration.setExposeGlobalJmxStatistics(true);
      globalConfiguration.setMBeanServerLookup(PerThreadMBeanServerLookup.class.getName());
      cm = TestCacheManagerFactory.createCacheManager(globalConfiguration);
      String jmxDomain = globalConfiguration.getJmxDomain();
      Configuration localCache = config();//local by default
      localCache.setExposeJmxStatistics(true);
      cm.defineConfiguration("local_cache", localCache);
      cm.getCache("local_cache");
      assert existsObject(jmxDomain + ":cache-name=\"local_cache(local)\",jmx-resource=Statistics");

      GlobalConfiguration globalConfiguration2 = GlobalConfiguration.getClusteredDefault();
      globalConfiguration2.setExposeGlobalJmxStatistics(true);
      globalConfiguration2.setAllowDuplicateDomains(true);
      globalConfiguration2.setMBeanServerLookup(PerThreadMBeanServerLookup.class.getName());
      cm2 = TestCacheManagerFactory.createCacheManager(globalConfiguration2);
      String jmxDomain2 = cm2.getGlobalConfiguration().getJmxDomain();
      Configuration localCache2 = config();//local by default
      localCache2.setExposeJmxStatistics(true);
      cm2.defineConfiguration("local_cache", localCache);
      cm2.getCache("local_cache");
      assert existsObject(jmxDomain2 + ":cache-name=\"local_cache(local)\",jmx-resource=Statistics");

      GlobalConfiguration globalConfiguration3 = GlobalConfiguration.getClusteredDefault();
      globalConfiguration3.setExposeGlobalJmxStatistics(true);
      globalConfiguration3.setAllowDuplicateDomains(true);
      globalConfiguration3.setMBeanServerLookup(PerThreadMBeanServerLookup.class.getName());
      cm3 = TestCacheManagerFactory.createCacheManager(globalConfiguration3);
      Configuration localCache3 = config();//local by default
      localCache3.setExposeJmxStatistics(true);
      cm3.defineConfiguration("local_cache", localCache);
      cm3.getCache("local_cache");
      String jmxDomain3 = cm3.getGlobalConfiguration().getJmxDomain();
      assert existsObject(jmxDomain3 + ":cache-name=\"local_cache(local)\",jmx-resource=Statistics");
   }

   public void testUnregisterJmxInfoOnStop() {
      GlobalConfiguration globalConfiguration = GlobalConfiguration.getClusteredDefault();
      globalConfiguration.setExposeGlobalJmxStatistics(true);
      globalConfiguration.setMBeanServerLookup(PerThreadMBeanServerLookup.class.getName());
      cm = TestCacheManagerFactory.createCacheManager(globalConfiguration);
      Configuration localCache = config();//local by default
      localCache.setExposeJmxStatistics(true);
      cm.defineConfiguration("local_cache", localCache);
      cm.getCache("local_cache");
      String jmxDomain = globalConfiguration.getJmxDomain();
      assert existsObject(jmxDomain + ":cache-name=\"local_cache(local)\",jmx-resource=Statistics");

      TestingUtil.killCacheManagers(cm);

      assert !existsObject(jmxDomain + ":cache-name=\"local_cache(local)\",jmx-resource=Statistics");
      assert !existsDomains(jmxDomain);
   }

   public void testCorrectUnregistering() {
      assert !existsDomains("infinispan");
      GlobalConfiguration globalConfiguration = GlobalConfiguration.getNonClusteredDefault();
      globalConfiguration.setMBeanServerLookup(PerThreadMBeanServerLookup.class.getName());
      cm = TestCacheManagerFactory.createCacheManager(globalConfiguration);
      Configuration localCache = config();//local by default
      cm.defineConfiguration("local_cache", localCache);
      cm.getCache("local_cache");
      String jmxDomain = cm.getGlobalConfiguration().getJmxDomain();
      assert existsObject(jmxDomain + ":cache-name=\"local_cache(local)\",jmx-resource=Statistics");
      assert existsObject(jmxDomain + ":cache-name=\"local_cache(local)\",jmx-resource=Cache");

      //now register a global one
      GlobalConfiguration globalConfiguration2 = GlobalConfiguration.getClusteredDefault();
      globalConfiguration2.setExposeGlobalJmxStatistics(true);
      globalConfiguration2.setMBeanServerLookup(PerThreadMBeanServerLookup.class.getName());
      globalConfiguration2.setAllowDuplicateDomains(true);
      cm2 = TestCacheManagerFactory.createCacheManager(globalConfiguration2);
      Configuration remoteCache = new Configuration();
      remoteCache.setExposeJmxStatistics(true);
      remoteCache.setCacheMode(Configuration.CacheMode.REPL_SYNC);
      cm2.defineConfiguration("remote_cache", remoteCache);
      cm2.getCache("remote_cache");
      String jmxDomain2 = cm2.getGlobalConfiguration().getJmxDomain();
      assert existsObject(jmxDomain2 + ":cache-name=\"remote_cache(repl_sync)\",jmx-resource=Cache");
      assert existsObject(jmxDomain2 + ":cache-name=\"remote_cache(repl_sync)\",jmx-resource=Statistics");

      cm2.stop();
      assert existsObject(jmxDomain + ":cache-name=\"local_cache(local)\",jmx-resource=Statistics");
      assert !existsObject(jmxDomain2 + ":cache-name=\"remote_cache(repl_sync)\",jmx-resource=Cache");
      assert !existsObject(jmxDomain2 + ":cache-name=\"remote_cache(repl_sync)\",jmx-resource=Statistics");

      cm.stop();
      assert !existsObject(jmxDomain + ":cache-name=\"local_cache(local)\",jmx-resource=Statistics");
      assert !existsObject(jmxDomain2 + ":cache-name=\"remote_cache(repl_sync)\",jmx-resource=Statistics");
   }

   public void testStopUnstartedCacheManager() {
      GlobalConfiguration globalConfiguration = GlobalConfiguration.getNonClusteredDefault();
      globalConfiguration.setExposeGlobalJmxStatistics(true);
      globalConfiguration.setMBeanServerLookup(PerThreadMBeanServerLookup.class.getName());
      cm = TestCacheManagerFactory.createCacheManager(false, globalConfiguration);
      cm.stop();
   }

   static boolean existsObject(String s) {
      try {
         ObjectName objectName = new ObjectName(s);
         return PerThreadMBeanServerLookup.getThreadMBeanServer().isRegistered(objectName);
      } catch (MalformedObjectNameException e) {
         throw new RuntimeException(e);
      }
   }

   public boolean existsDomains(String... domains) {
      MBeanServer mBeanServer = PerThreadMBeanServerLookup.getThreadMBeanServer();
      Set<String> domainSet = new HashSet<String>(Arrays.asList(domains));
      for (String domain : mBeanServer.getDomains()) {
         if (domainSet.contains(domain)) return true;
      }
      return false;
   }

   private Configuration config() {
      Configuration configuration = new Configuration();
      configuration.setFetchInMemoryState(false);
      configuration.setExposeJmxStatistics(true);
      return configuration;
   }
}
