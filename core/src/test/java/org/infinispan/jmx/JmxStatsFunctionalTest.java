package org.infinispan.jmx;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.CacheContainer;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.reflect.Method;
import java.util.Properties;

import static org.infinispan.test.TestingUtil.getCacheManagerObjectName;
import static org.infinispan.test.TestingUtil.getCacheObjectName;
import static org.infinispan.test.TestingUtil.existsObject;
import static org.infinispan.test.TestingUtil.existsDomains;

/**
 * Functional test for checking jmx statistics exposure.
 *
 * @author Mircea.Markus@jboss.com
 * @author Galder Zamarreño
 * @since 4.0
 */
@Test(groups = {"functional", "smoke"}, testName = "jmx.JmxStatsFunctionalTest")
public class JmxStatsFunctionalTest extends AbstractInfinispanTest {

   public static final String JMX_DOMAIN = JmxStatsFunctionalTest.class.getSimpleName();
   private MBeanServer server;
   private EmbeddedCacheManager cm, cm2, cm3;


   @AfterMethod
   public void destroyCacheManager() {
      TestingUtil.killCacheManagers(cm, cm2, cm3);
      cm = null;
      cm2 = null;
      cm3 = null;
      server = null;
   }

   /**
    * Create a local cache, two replicated caches and see that everithing is correctly registered.
    */
   public void testDefaultDomain() throws Exception {
      GlobalConfigurationBuilder globalConfiguration = GlobalConfigurationBuilder.defaultClusteredBuilder();
      globalConfiguration.globalJmxStatistics().enable().mBeanServerLookup(new PerThreadMBeanServerLookup());
      cm = TestCacheManagerFactory.createClusteredCacheManager(globalConfiguration, new ConfigurationBuilder());
      String jmxDomain = cm.getCacheManagerConfiguration().globalJmxStatistics().domain();

      ConfigurationBuilder localCache = config();//local by default
      cm.defineConfiguration("local_cache", localCache.build());
      ConfigurationBuilder remote1 = config();//local by default
      remote1.clustering().cacheMode(CacheMode.REPL_SYNC);
      cm.defineConfiguration("remote1", remote1.build());
      ConfigurationBuilder remote2 = config();//local by default
      remote2.clustering().cacheMode(CacheMode.INVALIDATION_ASYNC);
      cm.defineConfiguration("remote2", remote2.build());

      cm.getCache("local_cache");
      cm.getCache("remote1");
      cm.getCache("remote2");

      assert existsObject(getCacheObjectName(jmxDomain, "local_cache(local)", "Statistics"));
      assert existsObject(getCacheObjectName(jmxDomain, "remote1(repl_sync)", "RpcManager"));
      assert existsObject(getCacheObjectName(jmxDomain, "remote1(repl_sync)", "Statistics"));
      assert existsObject(getCacheObjectName(jmxDomain, "remote2(invalidation_async)", "RpcManager"));
      assert existsObject(getCacheObjectName(jmxDomain, "remote2(invalidation_async)", "Statistics"));

      TestingUtil.killCacheManagers(cm);

      assert !existsObject(getCacheObjectName(jmxDomain, "local_cache(local)", "Statistics"));
      assert !existsObject(getCacheObjectName(jmxDomain, "remote1(repl_sync)", "RpcManager"));
      assert !existsObject(getCacheObjectName(jmxDomain, "remote1(repl_sync)", "Statistics"));
      assert !existsObject(getCacheObjectName(jmxDomain, "remote2(invalidation_async)", "RpcManager"));
      assert !existsObject(getCacheObjectName(jmxDomain, "remote2(invalidation_async)", "Statistics"));
   }

   public void testDifferentDomain() throws Exception {
      GlobalConfigurationBuilder globalConfiguration = GlobalConfigurationBuilder.defaultClusteredBuilder();
      globalConfiguration.globalJmxStatistics().enable().mBeanServerLookup(new PerThreadMBeanServerLookup());
      cm = TestCacheManagerFactory.createClusteredCacheManager(globalConfiguration, new ConfigurationBuilder());
      String jmxDomain = cm.getCacheManagerConfiguration().globalJmxStatistics().domain();

      ConfigurationBuilder localCache = config();//local by default
      cm.defineConfiguration("local_cache", localCache.build());
      cm.getCache("local_cache");

      assert existsObject(getCacheObjectName(jmxDomain, "local_cache(local)", "Statistics"));
   }


   public void testOnlyGlobalJmxStatsEnabled() throws Exception {
      GlobalConfigurationBuilder globalConfiguration = GlobalConfigurationBuilder.defaultClusteredBuilder();
      globalConfiguration.globalJmxStatistics().enable().mBeanServerLookup(new PerThreadMBeanServerLookup());
      cm = TestCacheManagerFactory.createClusteredCacheManager(globalConfiguration, new ConfigurationBuilder());
      String jmxDomain = cm.getCacheManagerConfiguration().globalJmxStatistics().domain();

      ConfigurationBuilder localCache = config();//local by default
      localCache.jmxStatistics().disable();
      cm.defineConfiguration("local_cache", localCache.build());
      ConfigurationBuilder remote1 = config();//local by default
      remote1.jmxStatistics().disable();
      remote1.clustering().cacheMode(CacheMode.REPL_SYNC);
      cm.defineConfiguration("remote1", remote1.build());

      cm.getCache("local_cache");
      cm.getCache("remote1");

      assert existsObject(getCacheManagerObjectName(jmxDomain));

      // Statistics MBean is always enabled now
      assert existsObject(getCacheObjectName(jmxDomain, "local_cache(local)", "Statistics"));
      assert existsObject(getCacheObjectName(jmxDomain, "remote1(repl_sync)", "Statistics"));

      // Since ISPN-2290
      assert existsObject(getCacheObjectName(jmxDomain, "remote1(repl_sync)", "LockManager"));
      assert existsObject(getCacheObjectName(jmxDomain, "local_cache(local)", "LockManager"));


   }

   public void testOnlyPerCacheJmxStatsEnabled() throws Exception {
      GlobalConfigurationBuilder globalConfiguration = GlobalConfigurationBuilder.defaultClusteredBuilder();
      globalConfiguration.globalJmxStatistics().enable().mBeanServerLookup(new PerThreadMBeanServerLookup());
      cm = TestCacheManagerFactory.createClusteredCacheManager(globalConfiguration, new ConfigurationBuilder());
      String jmxDomain = cm.getCacheManagerConfiguration().globalJmxStatistics().domain();

      ConfigurationBuilder localCache = config();//local by default
      localCache.jmxStatistics().enable();
      cm.defineConfiguration("local_cache", localCache.build());
      ConfigurationBuilder remote1 = config();//local by default
      remote1.jmxStatistics().enable();
      remote1.clustering().cacheMode(CacheMode.REPL_SYNC);
      cm.defineConfiguration("remote1", remote1.build());

      cm.getCache("local_cache");
      cm.getCache("remote1");

      assert existsObject(getCacheObjectName(jmxDomain, "local_cache(local)", "Statistics"));
      // Since ISPN-2290
      assert existsObject(getCacheManagerObjectName(jmxDomain));
      assert existsObject(getCacheObjectName(jmxDomain, "remote1(repl_sync)", "RpcManager"));
   }

   public void testMultipleManagersOnSameServerFails(Method method) throws Exception {

      final String jmxDomain = JMX_DOMAIN + '.' + method.getName();
      cm = TestCacheManagerFactory.createClusteredCacheManagerEnforceJmxDomain(jmxDomain);
      ConfigurationBuilder localCache = config();//local by default
      localCache.jmxStatistics().enable();
      cm.defineConfiguration("local_cache", localCache.build());
      cm.getCache("local_cache");
      assert existsObject(getCacheObjectName(jmxDomain, "local_cache(local)", "Statistics"));

      try {
         TestCacheManagerFactory.createClusteredCacheManagerEnforceJmxDomain(jmxDomain, false);
         assert false : "Failure expected, '" + jmxDomain + "' duplicate!";
      } catch (JmxDomainConflictException e) {
      }

      server = PerThreadMBeanServerLookup.getThreadMBeanServer();
      CacheContainer duplicateAllowedContainer =TestCacheManagerFactory.createClusteredCacheManagerEnforceJmxDomain(jmxDomain, true);
      try {
         final String duplicateName = jmxDomain + "2";
         ObjectName duplicateObjectName = getCacheManagerObjectName(duplicateName);
         server.getAttribute(duplicateObjectName, "CreatedCacheCount").equals("0");
      } finally {
         duplicateAllowedContainer.stop();
      }
   }

   public void testMultipleManagersOnSameServerWithCloneFails() throws Exception {
      GlobalConfigurationBuilder globalConfiguration = GlobalConfigurationBuilder.defaultClusteredBuilder();
      globalConfiguration.globalJmxStatistics().enable().mBeanServerLookup(new PerThreadMBeanServerLookup());
      cm = TestCacheManagerFactory.createClusteredCacheManager(globalConfiguration, new ConfigurationBuilder());
      String jmxDomain = cm.getCacheManagerConfiguration().globalJmxStatistics().domain();
      ConfigurationBuilder localCache = config();//local by default
      localCache.jmxStatistics().enable();
      cm.defineConfiguration("local_cache", localCache.build());
      cm.getCache("local_cache");
      assert existsObject(getCacheObjectName(jmxDomain, "local_cache(local)", "Statistics"));

      try {
         TestCacheManagerFactory.createClusteredCacheManagerEnforceJmxDomain(jmxDomain, false);
         assert false : "Failure expected!";
      } catch (JmxDomainConflictException e) {
      }
   }

   public void testMultipleManagersOnSameServer() throws Exception {
      GlobalConfigurationBuilder globalConfiguration = GlobalConfigurationBuilder.defaultClusteredBuilder();
      globalConfiguration.globalJmxStatistics().enable().mBeanServerLookup(new PerThreadMBeanServerLookup());
      cm = TestCacheManagerFactory.createClusteredCacheManager(globalConfiguration, new ConfigurationBuilder());
      String jmxDomain = cm.getCacheManagerConfiguration().globalJmxStatistics().domain();
      ConfigurationBuilder localCache = config();//local by default
      localCache.jmxStatistics().enable();
      cm.defineConfiguration("local_cache", localCache.build());
      cm.getCache("local_cache");
      assert existsObject(getCacheObjectName(jmxDomain, "local_cache(local)", "Statistics"));

      GlobalConfigurationBuilder globalConfiguration2 = GlobalConfigurationBuilder.defaultClusteredBuilder();
      globalConfiguration2.globalJmxStatistics().enable().mBeanServerLookup(new PerThreadMBeanServerLookup()).allowDuplicateDomains(true);
      cm2 = TestCacheManagerFactory.createClusteredCacheManager(globalConfiguration2, new ConfigurationBuilder());
      String jmxDomain2 = cm2.getCacheManagerConfiguration().globalJmxStatistics().domain();

      ConfigurationBuilder localCache2 = config();//local by default
      localCache2.jmxStatistics().enable();
      cm2.defineConfiguration("local_cache", localCache.build());
      cm2.getCache("local_cache");
      assert existsObject(getCacheObjectName(jmxDomain2, "local_cache(local)", "Statistics"));

      GlobalConfigurationBuilder globalConfiguration3 = GlobalConfigurationBuilder.defaultClusteredBuilder();
      globalConfiguration3.globalJmxStatistics().enable().mBeanServerLookup(new PerThreadMBeanServerLookup()).allowDuplicateDomains(true);
      cm3 = TestCacheManagerFactory.createClusteredCacheManager(globalConfiguration3, new ConfigurationBuilder());
      String jmxDomain3 = cm3.getCacheManagerConfiguration().globalJmxStatistics().domain();

      ConfigurationBuilder localCache3 = config();//local by default
      localCache3.jmxStatistics().enable();
      cm3.defineConfiguration("local_cache", localCache.build());
      cm3.getCache("local_cache");
      assert existsObject(getCacheObjectName(jmxDomain3, "local_cache(local)", "Statistics"));
   }

   public void testUnregisterJmxInfoOnStop() throws Exception {
      GlobalConfigurationBuilder globalConfiguration = GlobalConfigurationBuilder.defaultClusteredBuilder();
      globalConfiguration.globalJmxStatistics().enable().mBeanServerLookup(new PerThreadMBeanServerLookup());
      cm = TestCacheManagerFactory.createClusteredCacheManager(globalConfiguration, new ConfigurationBuilder());
      String jmxDomain = cm.getCacheManagerConfiguration().globalJmxStatistics().domain();
      ConfigurationBuilder localCache = config();//local by default
      localCache.jmxStatistics().enable();
      cm.defineConfiguration("local_cache", localCache.build());
      cm.getCache("local_cache");
      assert existsObject(getCacheObjectName(jmxDomain, "local_cache(local)", "Statistics"));

      TestingUtil.killCacheManagers(cm);

      assert !existsObject(getCacheObjectName(jmxDomain, "local_cache(local)", "Statistics"));
      assert !existsDomains(jmxDomain);
   }

   public void testCorrectUnregistering() throws Exception {
      assert !existsDomains("infinispan");
      GlobalConfigurationBuilder globalConfiguration = new GlobalConfigurationBuilder();
      globalConfiguration.globalJmxStatistics().enable().mBeanServerLookup(new PerThreadMBeanServerLookup());
      cm = TestCacheManagerFactory.createCacheManager(globalConfiguration, new ConfigurationBuilder());
      ConfigurationBuilder localCache = config();//local by default
      cm.defineConfiguration("local_cache", localCache.build());
      cm.getCache("local_cache");
      String jmxDomain = cm.getCacheManagerConfiguration().globalJmxStatistics().domain();
      assert existsObject(getCacheObjectName(jmxDomain, "local_cache(local)", "Statistics"));
      assert existsObject(getCacheObjectName(jmxDomain, "local_cache(local)", "Cache"));

      //now register a global one
      GlobalConfigurationBuilder globalConfiguration2 = GlobalConfigurationBuilder.defaultClusteredBuilder();
      globalConfiguration2.globalJmxStatistics().enable().mBeanServerLookup(new PerThreadMBeanServerLookup()).allowDuplicateDomains(true);
      cm2 = TestCacheManagerFactory.createClusteredCacheManager(globalConfiguration2, new ConfigurationBuilder());
      ConfigurationBuilder remoteCache = new ConfigurationBuilder();
      remoteCache.jmxStatistics().enable();
      remoteCache.clustering().cacheMode(CacheMode.REPL_SYNC);
      cm2.defineConfiguration("remote_cache", remoteCache.build());
      cm2.getCache("remote_cache");
      String jmxDomain2 = cm2.getCacheManagerConfiguration().globalJmxStatistics().domain();
      assert existsObject(getCacheObjectName(jmxDomain2, "remote_cache(repl_sync)", "Cache"));
      assert existsObject(getCacheObjectName(jmxDomain2, "remote_cache(repl_sync)", "Statistics"));

      cm2.stop();
      assert existsObject(getCacheObjectName(jmxDomain, "local_cache(local)", "Statistics"));
      assert !existsObject(getCacheObjectName(jmxDomain2, "remote_cache(repl_sync)", "CacheComponent"));
      assert !existsObject(getCacheObjectName(jmxDomain2, "remote_cache(repl_sync)", "Statistics"));

      cm.stop();
      assert !existsObject(getCacheObjectName(jmxDomain, "local_cache(local)", "Statistics"));
      assert !existsObject(getCacheObjectName(jmxDomain2, "remote_cache(repl_sync)", "Statistics"));
   }

   public void testStopUnstartedCacheManager() {
      GlobalConfigurationBuilder globalConfiguration = new GlobalConfigurationBuilder();
      globalConfiguration.globalJmxStatistics().enable().mBeanServerLookup(new PerThreadMBeanServerLookup());
      cm = TestCacheManagerFactory.createCacheManager(globalConfiguration, new ConfigurationBuilder(), false);
      cm.stop();
   }

   public void testConfigurationProperties() throws Exception {
      GlobalConfigurationBuilder globalConfiguration = GlobalConfigurationBuilder.defaultClusteredBuilder();
      globalConfiguration.transport().siteId("TESTVALUE1");
      globalConfiguration.transport().rackId("TESTVALUE2");
      globalConfiguration.transport().machineId("TESTVALUE3");
      globalConfiguration.globalJmxStatistics().enable().mBeanServerLookup(new PerThreadMBeanServerLookup());
      cm = TestCacheManagerFactory.createClusteredCacheManager(globalConfiguration, new ConfigurationBuilder());
      String jmxDomain = cm.getCacheManagerConfiguration().globalJmxStatistics().domain();

      ConfigurationBuilder localCache = config();
      localCache.storeAsBinary().enable();
      cm.defineConfiguration("local_cache1", localCache.build());
      localCache.storeAsBinary().disable();
      cm.defineConfiguration("local_cache2", localCache.build());

      cm.getCache("local_cache1");
      cm.getCache("local_cache2");

      MBeanServer mBeanServer = PerThreadMBeanServerLookup.getThreadMBeanServer();
      Properties props1 = (Properties) mBeanServer.getAttribute(getCacheObjectName(jmxDomain, "local_cache1(local)", "Cache"), "configurationAsProperties");
      Properties props2 = (Properties) mBeanServer.getAttribute(getCacheObjectName(jmxDomain, "local_cache2(local)", "Cache"), "configurationAsProperties");
      Properties propsGlobal = (Properties) mBeanServer.getAttribute(getCacheManagerObjectName(jmxDomain), "globalConfigurationAsProperties");
      assert "true".equals(props1.getProperty("storeAsBinary.enabled"));
      assert "false".equals(props2.getProperty("storeAsBinary.enabled"));
      log.tracef("propsGlobal=%s", propsGlobal);
      assert "TESTVALUE1".equals(propsGlobal.getProperty("transport.siteId"));
      assert "TESTVALUE2".equals(propsGlobal.getProperty("transport.rackId"));
      assert "TESTVALUE3".equals(propsGlobal.getProperty("transport.machineId"));
   }

   private ConfigurationBuilder config() {
      ConfigurationBuilder configuration = new ConfigurationBuilder();
      configuration.clustering().stateTransfer().fetchInMemoryState(false).jmxStatistics().enable();
      return configuration;
   }
}
