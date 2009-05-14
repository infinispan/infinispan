package org.infinispan.jmx;

import org.infinispan.CacheException;
import org.infinispan.config.Configuration;
import org.infinispan.config.GlobalConfiguration;
import org.infinispan.manager.CacheManager;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
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
public class JmxStatsFunctionalTest {

   private CacheManager cm, cm2, cm3;


   @AfterMethod
   public void destroyCacheManager() {
      TestingUtil.killCacheManagers(cm, cm2, cm3);
      assert !existsDomains("infinispan");
   }

   /**
    * Create a local cache, two replicated caches and see that everithing is correctly registered.
    */
   public void testDefaultDomain() {
      assert !existsDomains("infinispan");
      GlobalConfiguration globalConfiguration = GlobalConfiguration.getClusteredDefault();
      globalConfiguration.setExposeGlobalJmxStatistics(true);
      globalConfiguration.setMBeanServerLookup(PerThreadMBeanServerLookup.class.getName());
      cm = TestCacheManagerFactory.createCacheManager(globalConfiguration);

      Configuration localCache = config();//local by default
      cm.defineCache("local_cache", localCache);
      Configuration remote1 = config();//local by default
      remote1.setCacheMode(Configuration.CacheMode.REPL_SYNC);
      cm.defineCache("remote1", remote1);
      Configuration remote2 = config();//local by default
      remote2.setCacheMode(Configuration.CacheMode.INVALIDATION_ASYNC);
      cm.defineCache("remote2", remote2);

      cm.getCache("local_cache");
      cm.getCache("remote1");
      cm.getCache("remote2");

      assert existsObject("infinispan:cache-name=local_cache(local),jmx-resource=CacheMgmtInterceptor");
      assert existsObject("infinispan:cache-name=remote1(repl_sync),jmx-resource=RpcManager");
      assert existsObject("infinispan:cache-name=remote1(repl_sync),jmx-resource=CacheMgmtInterceptor");
      assert existsObject("infinispan:cache-name=remote2(invalidation_async),jmx-resource=RpcManager");
      assert existsObject("infinispan:cache-name=remote2(invalidation_async),jmx-resource=CacheMgmtInterceptor");

      TestingUtil.killCacheManagers(cm);

      assert !existsObject("infinispan:cache-name=local_cache(local),jmx-resource=CacheMgmtInterceptor");
      assert !existsObject("infinispan:cache-name=remote1(repl_sync),jmx-resource=RpcManager");
      assert !existsObject("infinispan:cache-name=remote1(repl_sync),jmx-resource=CacheMgmtInterceptor");
      assert !existsObject("infinispan:cache-name=remote2(invalidation_async),jmx-resource=RpcManager");
      assert !existsObject("infinispan:cache-name=remote2(invalidation_async),jmx-resource=CacheMgmtInterceptor");
   }

   public void testDifferentDomain() {
      assert !existsDomains("infinispan");
      assert !existsDomains("mircea_jmx_domain");
      GlobalConfiguration globalConfiguration = GlobalConfiguration.getClusteredDefault();
      globalConfiguration.setJmxDomain("mircea_jmx_domain");
      globalConfiguration.setMBeanServerLookup(PerThreadMBeanServerLookup.class.getName());
      cm = TestCacheManagerFactory.createCacheManager(globalConfiguration);

      Configuration localCache = config();//local by default
      cm.defineCache("local_cache", localCache);
      cm.getCache("local_cache");

      assert existsObject("mircea_jmx_domain:cache-name=local_cache(local),jmx-resource=CacheMgmtInterceptor");
   }


   public void testOnlyGlobalJmxStatsEnabled() {
      assert !existsDomains("infinispan");
      GlobalConfiguration globalConfiguration = GlobalConfiguration.getClusteredDefault();
      globalConfiguration.setExposeGlobalJmxStatistics(true);
      globalConfiguration.setMBeanServerLookup(PerThreadMBeanServerLookup.class.getName());
      cm = TestCacheManagerFactory.createCacheManager(globalConfiguration);

      Configuration localCache = config();//local by default
      localCache.setExposeJmxStatistics(false);
      cm.defineCache("local_cache", localCache);
      Configuration remote1 = config();//local by default
      remote1.setExposeJmxStatistics(false);
      remote1.setCacheMode(Configuration.CacheMode.REPL_SYNC);
      cm.defineCache("remote1", remote1);

      cm.getCache("local_cache");
      cm.getCache("remote1");

      assert !existsObject("infinispan:cache-name=local_cache(local),jmx-resource=CacheMgmtInterceptor");
      assert existsObject("infinispan:cache-name=[global],jmx-resource=CacheManager");
      assert !existsObject("infinispan:cache-name=remote1(repl_sync),jmx-resource=CacheMgmtInterceptor");
   }

   public void testOnlyPerCacheJmxStatsEnabled() {
      assert !existsDomains("infinispan");
      GlobalConfiguration globalConfiguration = GlobalConfiguration.getClusteredDefault();
      globalConfiguration.setExposeGlobalJmxStatistics(false);
      globalConfiguration.setMBeanServerLookup(PerThreadMBeanServerLookup.class.getName());
      cm = TestCacheManagerFactory.createCacheManager(globalConfiguration);

      Configuration localCache = config();//local by default
      localCache.setExposeJmxStatistics(true);
      cm.defineCache("local_cache", localCache);
      Configuration remote1 = config();//local by default
      remote1.setExposeJmxStatistics(true);
      remote1.setCacheMode(Configuration.CacheMode.REPL_SYNC);
      cm.defineCache("remote1", remote1);

      cm.getCache("local_cache");
      cm.getCache("remote1");

      assert existsObject("infinispan:cache-name=local_cache(local),jmx-resource=CacheMgmtInterceptor");
      assert !existsObject("infinispan:cache-name=[global],jmx-resource=RpcManager");
      assert existsObject("infinispan:cache-name=remote1(repl_sync),jmx-resource=CacheMgmtInterceptor");
   }

   public void testMultipleManagersOnSameServerFails() {
      assert !existsDomains("infinispan");
      GlobalConfiguration globalConfiguration = GlobalConfiguration.getClusteredDefault();
      globalConfiguration.setMBeanServerLookup(PerThreadMBeanServerLookup.class.getName());
      cm = TestCacheManagerFactory.createCacheManager(globalConfiguration);
      Configuration localCache = config();//local by default
      localCache.setExposeJmxStatistics(true);
      cm.defineCache("local_cache", localCache);
      cm.getCache("local_cache");
      assert existsObject("infinispan:cache-name=local_cache(local),jmx-resource=CacheMgmtInterceptor");

      GlobalConfiguration globalConfiguration2 = GlobalConfiguration.getClusteredDefault();
      globalConfiguration2.setExposeGlobalJmxStatistics(true);
      globalConfiguration2.setAllowDuplicateDomains(false);
      globalConfiguration2.setMBeanServerLookup(PerThreadMBeanServerLookup.class.getName());
      cm2 = TestCacheManagerFactory.createCacheManager(globalConfiguration);
      Configuration localCache2 = config();//local by default
      localCache2.setExposeJmxStatistics(true);
      cm2.defineCache("local_cache", localCache);
      try {
         cm2.getCache("local_cache");
         assert false : "exception expected";
      } catch (CacheException e) {
         //expected
      }
   }

   public void testMultipleManagersOnSameServer() {
      assert !existsDomains("infinispan");
      GlobalConfiguration globalConfiguration = GlobalConfiguration.getClusteredDefault();
      globalConfiguration.setAllowDuplicateDomains(true);
      globalConfiguration.setExposeGlobalJmxStatistics(true);
      globalConfiguration.setMBeanServerLookup(PerThreadMBeanServerLookup.class.getName());
      cm = TestCacheManagerFactory.createCacheManager(globalConfiguration);
      Configuration localCache = config();//local by default
      localCache.setExposeJmxStatistics(true);
      cm.defineCache("local_cache", localCache);
      cm.getCache("local_cache");
      assert existsObject("infinispan:cache-name=local_cache(local),jmx-resource=CacheMgmtInterceptor");

      GlobalConfiguration globalConfiguration2 = GlobalConfiguration.getClusteredDefault();
      globalConfiguration2.setExposeGlobalJmxStatistics(true);
      globalConfiguration2.setAllowDuplicateDomains(true);
      globalConfiguration2.setMBeanServerLookup(PerThreadMBeanServerLookup.class.getName());
      cm2 = TestCacheManagerFactory.createCacheManager(globalConfiguration2);
      Configuration localCache2 = config();//local by default
      localCache2.setExposeJmxStatistics(true);
      cm2.defineCache("local_cache", localCache);
      cm2.getCache("local_cache");
      assert existsObject("infinispan2:cache-name=local_cache(local),jmx-resource=CacheMgmtInterceptor");

      GlobalConfiguration globalConfiguration3 = GlobalConfiguration.getClusteredDefault();
      globalConfiguration3.setExposeGlobalJmxStatistics(true);
      globalConfiguration3.setAllowDuplicateDomains(true);
      globalConfiguration3.setMBeanServerLookup(PerThreadMBeanServerLookup.class.getName());
      cm3 = TestCacheManagerFactory.createCacheManager(globalConfiguration3);
      Configuration localCache3 = config();//local by default
      localCache3.setExposeJmxStatistics(true);
      cm3.defineCache("local_cache", localCache);
      cm3.getCache("local_cache");
      assert existsObject("infinispan3:cache-name=local_cache(local),jmx-resource=CacheMgmtInterceptor");
   }

   public void testUnregisterJmxInfoOnStop() {
      assert !existsDomains("infinispan");
      GlobalConfiguration globalConfiguration = GlobalConfiguration.getClusteredDefault();
      globalConfiguration.setExposeGlobalJmxStatistics(true);
      globalConfiguration.setMBeanServerLookup(PerThreadMBeanServerLookup.class.getName());
      cm = TestCacheManagerFactory.createCacheManager(globalConfiguration);
      Configuration localCache = config();//local by default
      localCache.setExposeJmxStatistics(true);
      cm.defineCache("local_cache", localCache);
      cm.getCache("local_cache");
      assert existsObject("infinispan:cache-name=local_cache(local),jmx-resource=CacheMgmtInterceptor");

      TestingUtil.killCacheManagers(cm);

      assert !existsObject("infinispan:cache-name=local_cache(local),jmx-resource=CacheMgmtInterceptor");
      assert !existsDomains("infinispan");
   }

   public void testCorrectUnregistering() {
      assert !existsDomains("infinispan");
      GlobalConfiguration globalConfiguration = GlobalConfiguration.getNonClusteredDefault();
      globalConfiguration.setMBeanServerLookup(PerThreadMBeanServerLookup.class.getName());
      cm = TestCacheManagerFactory.createCacheManager(globalConfiguration);
      Configuration localCache = config();//local by default
      cm.defineCache("local_cache", localCache);
      cm.getCache("local_cache");
      assert existsObject("infinispan:cache-name=local_cache(local),jmx-resource=CacheMgmtInterceptor");

      //now register a global one
      GlobalConfiguration globalConfiguration2 = GlobalConfiguration.getClusteredDefault();
      globalConfiguration2.setExposeGlobalJmxStatistics(true);
      globalConfiguration2.setMBeanServerLookup(PerThreadMBeanServerLookup.class.getName());
      globalConfiguration2.setAllowDuplicateDomains(true);
      cm2 = TestCacheManagerFactory.createCacheManager(globalConfiguration2);
      Configuration remoteCache = new Configuration();
      remoteCache.setExposeJmxStatistics(true);
      remoteCache.setCacheMode(Configuration.CacheMode.REPL_SYNC);
      cm2.defineCache("remote_cache", remoteCache);
      cm2.getCache("remote_cache");
      assert existsObject("infinispan2:cache-name=remote_cache(repl_sync),jmx-resource=CacheMgmtInterceptor");

      cm2.stop();
      assert existsObject("infinispan:cache-name=local_cache(local),jmx-resource=CacheMgmtInterceptor");
      assert !existsObject("infinispan2:cache-name=remote_cache(repl_sync),jmx-resource=CacheMgmtInterceptor");

      cm.stop();
      assert !existsObject("infinispan:cache-name=local_cache(local),jmx-resource=CacheMgmtInterceptor");
      assert !existsObject("infinispan2:cache-name=remote_cache(repl_sync),jmx-resource=CacheMgmtInterceptor");
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
