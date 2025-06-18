package org.infinispan.jmx;

import static org.infinispan.commons.test.Exceptions.expectException;
import static org.infinispan.test.TestingUtil.existsDomain;
import static org.infinispan.test.TestingUtil.getCacheManagerObjectName;
import static org.infinispan.test.TestingUtil.getCacheObjectName;
import static org.infinispan.test.fwk.TestCacheManagerFactory.configureJmx;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import java.lang.reflect.Method;
import java.util.Properties;

import javax.management.MBeanServer;

import org.infinispan.commons.jmx.MBeanServerLookup;
import org.infinispan.commons.jmx.TestMBeanServerLookup;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.StorageType;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.manager.EmbeddedCacheManagerStartupException;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

/**
 * Functional test for checking jmx statistics exposure.
 *
 * @author Mircea.Markus@jboss.com
 * @author Galder Zamarreño
 * @since 4.0
 */
@Test(groups = {"functional", "smoke"}, testName = "jmx.JmxStatsFunctionalTest")
public class JmxStatsFunctionalTest extends AbstractInfinispanTest {

   private static final String JMX_DOMAIN = JmxStatsFunctionalTest.class.getSimpleName();
   private final MBeanServerLookup mBeanServerLookup = TestMBeanServerLookup.create();
   private final MBeanServer server = mBeanServerLookup.getMBeanServer();
   private EmbeddedCacheManager cm, cm2, cm3;

   @AfterMethod(alwaysRun = true)
   public void destroyCacheManager() {
      TestingUtil.killCacheManagers(cm, cm2, cm3);
      cm = null;
      cm2 = null;
      cm3 = null;
   }

   /**
    * Create a local cache, two replicated caches and see that everything is correctly registered.
    */
   public void testDefaultDomain() {
      GlobalConfigurationBuilder globalConfiguration = GlobalConfigurationBuilder.defaultClusteredBuilder();
      configureJmx(globalConfiguration, JMX_DOMAIN, mBeanServerLookup);
      cm = TestCacheManagerFactory.createClusteredCacheManager(globalConfiguration, new ConfigurationBuilder());
      String jmxDomain = cm.getCacheManagerConfiguration().jmx().domain();

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

      assertTrue(server.isRegistered(getCacheObjectName(jmxDomain, "local_cache(local)", "Statistics")));
      assertTrue(server.isRegistered(getCacheObjectName(jmxDomain, "remote1(repl_sync)", "RpcManager")));
      assertTrue(server.isRegistered(getCacheObjectName(jmxDomain, "remote1(repl_sync)", "Statistics")));
      assertTrue(server.isRegistered(getCacheObjectName(jmxDomain, "remote2(invalidation_async)", "RpcManager")));
      assertTrue(server.isRegistered(getCacheObjectName(jmxDomain, "remote2(invalidation_async)", "Statistics")));

      TestingUtil.killCacheManagers(cm);

      assertFalse(server.isRegistered(getCacheObjectName(jmxDomain, "local_cache(local)", "Statistics")));
      assertFalse(server.isRegistered(getCacheObjectName(jmxDomain, "remote1(repl_sync)", "RpcManager")));
      assertFalse(server.isRegistered(getCacheObjectName(jmxDomain, "remote1(repl_sync)", "Statistics")));
      assertFalse(server.isRegistered(getCacheObjectName(jmxDomain, "remote2(invalidation_async)", "RpcManager")));
      assertFalse(server.isRegistered(getCacheObjectName(jmxDomain, "remote2(invalidation_async)", "Statistics")));
   }

   public void testDifferentDomain() {
      GlobalConfigurationBuilder globalConfiguration = GlobalConfigurationBuilder.defaultClusteredBuilder();
      configureJmx(globalConfiguration, JMX_DOMAIN, mBeanServerLookup);
      cm = TestCacheManagerFactory.createClusteredCacheManager(globalConfiguration, new ConfigurationBuilder());
      String jmxDomain = cm.getCacheManagerConfiguration().jmx().domain();

      ConfigurationBuilder localCache = config();//local by default
      cm.defineConfiguration("local_cache", localCache.build());
      cm.getCache("local_cache");

      assertTrue(server.isRegistered(getCacheObjectName(jmxDomain, "local_cache(local)", "Statistics")));
   }

   public void testOnlyGlobalJmxStatsEnabled() {
      GlobalConfigurationBuilder globalConfiguration = GlobalConfigurationBuilder.defaultClusteredBuilder();
      configureJmx(globalConfiguration, JMX_DOMAIN, mBeanServerLookup);
      cm = TestCacheManagerFactory.createClusteredCacheManager(globalConfiguration, new ConfigurationBuilder());
      String jmxDomain = cm.getCacheManagerConfiguration().jmx().domain();

      ConfigurationBuilder localCache = config();//local by default
      localCache.statistics().disable();
      cm.defineConfiguration("local_cache", localCache.build());
      ConfigurationBuilder remote1 = config();//local by default
      remote1.statistics().disable();
      remote1.clustering().cacheMode(CacheMode.REPL_SYNC);
      cm.defineConfiguration("remote1", remote1.build());

      cm.getCache("local_cache");
      cm.getCache("remote1");

      assertTrue(server.isRegistered(getCacheManagerObjectName(jmxDomain)));

      // Statistics MBean is always enabled now
      assertTrue(server.isRegistered(getCacheObjectName(jmxDomain, "local_cache(local)", "Statistics")));
      assertTrue(server.isRegistered(getCacheObjectName(jmxDomain, "remote1(repl_sync)", "Statistics")));

      // Since ISPN-2290
      assertTrue(server.isRegistered(getCacheObjectName(jmxDomain, "remote1(repl_sync)", "LockManager")));
      assertTrue(server.isRegistered(getCacheObjectName(jmxDomain, "local_cache(local)", "LockManager")));
   }

   public void testOnlyPerCacheJmxStatsEnabled() {
      GlobalConfigurationBuilder globalConfiguration = GlobalConfigurationBuilder.defaultClusteredBuilder();
      configureJmx(globalConfiguration, JMX_DOMAIN, mBeanServerLookup);
      cm = TestCacheManagerFactory.createClusteredCacheManager(globalConfiguration, new ConfigurationBuilder());
      String jmxDomain = cm.getCacheManagerConfiguration().jmx().domain();

      ConfigurationBuilder localCache = config();//local by default
      localCache.statistics().enable();
      cm.defineConfiguration("local_cache", localCache.build());
      ConfigurationBuilder remote1 = config();//local by default
      remote1.statistics().enable();
      remote1.clustering().cacheMode(CacheMode.REPL_SYNC);
      cm.defineConfiguration("remote1", remote1.build());

      cm.getCache("local_cache");
      cm.getCache("remote1");

      assertTrue(server.isRegistered(getCacheObjectName(jmxDomain, "local_cache(local)", "Statistics")));
      // Since ISPN-2290
      assertTrue(server.isRegistered(getCacheManagerObjectName(jmxDomain)));
      assertTrue(server.isRegistered(getCacheObjectName(jmxDomain, "remote1(repl_sync)", "RpcManager")));
   }

   public void testMultipleManagersOnSameServerFails(Method method) {
      final String jmxDomain = JMX_DOMAIN + '_' + method.getName();
      GlobalConfigurationBuilder globalConfiguration = GlobalConfigurationBuilder.defaultClusteredBuilder();
      configureJmx(globalConfiguration, jmxDomain, mBeanServerLookup);
      cm = TestCacheManagerFactory.createClusteredCacheManager(globalConfiguration, new ConfigurationBuilder());

      ConfigurationBuilder localCache = config();//local by default
      localCache.statistics().enable();
      cm.defineConfiguration("local_cache", localCache.build());
      cm.getCache("local_cache");
      assertTrue(server.isRegistered(getCacheObjectName(jmxDomain, "local_cache(local)", "Statistics")));

      GlobalConfigurationBuilder globalConfiguration2 = GlobalConfigurationBuilder.defaultClusteredBuilder();
      configureJmx(globalConfiguration2, jmxDomain, mBeanServerLookup);
      expectException(EmbeddedCacheManagerStartupException.class, JmxDomainConflictException.class,
            () -> TestCacheManagerFactory.createClusteredCacheManager(globalConfiguration2, new ConfigurationBuilder()));
   }

   public void testMultipleManagersOnSameServerWithCloneFails() {
      GlobalConfigurationBuilder globalConfiguration = GlobalConfigurationBuilder.defaultClusteredBuilder();
      configureJmx(globalConfiguration, JMX_DOMAIN, mBeanServerLookup);
      cm = TestCacheManagerFactory.createClusteredCacheManager(globalConfiguration, new ConfigurationBuilder());
      String jmxDomain = cm.getCacheManagerConfiguration().jmx().domain();
      ConfigurationBuilder localCache = config();//local by default
      localCache.statistics().enable();
      cm.defineConfiguration("local_cache", localCache.build());
      cm.getCache("local_cache");
      assertTrue(server.isRegistered(getCacheObjectName(jmxDomain, "local_cache(local)", "Statistics")));

      GlobalConfigurationBuilder globalConfiguration2 = new GlobalConfigurationBuilder();
      globalConfiguration2.read(globalConfiguration.build());
      globalConfiguration2.transport().defaultTransport();
      expectException(EmbeddedCacheManagerStartupException.class, JmxDomainConflictException.class,
            () -> TestCacheManagerFactory.createClusteredCacheManager(globalConfiguration2, new ConfigurationBuilder()));
   }

   public void testMultipleManagersOnSameServer() {
      String jmxDomain = JMX_DOMAIN;
      GlobalConfigurationBuilder globalConfiguration = GlobalConfigurationBuilder.defaultClusteredBuilder();
      configureJmx(globalConfiguration, jmxDomain, mBeanServerLookup);
      cm = TestCacheManagerFactory.createClusteredCacheManager(globalConfiguration, new ConfigurationBuilder());
      ConfigurationBuilder localCache = config();//local by default
      localCache.statistics().enable();
      cm.defineConfiguration("local_cache", localCache.build());
      cm.getCache("local_cache");
      assertTrue(server.isRegistered(getCacheObjectName(jmxDomain, "local_cache(local)", "Statistics")));

      String jmxDomain2 = JMX_DOMAIN + 2;
      GlobalConfigurationBuilder globalConfiguration2 = GlobalConfigurationBuilder.defaultClusteredBuilder();
      configureJmx(globalConfiguration2, jmxDomain2, mBeanServerLookup);
      cm2 = TestCacheManagerFactory.createClusteredCacheManager(globalConfiguration2, new ConfigurationBuilder());

      ConfigurationBuilder localCache2 = config();//local by default
      localCache2.statistics().enable();
      cm2.defineConfiguration("local_cache", localCache.build());
      cm2.getCache("local_cache");
      assertTrue(server.isRegistered(getCacheObjectName(jmxDomain2, "local_cache(local)", "Statistics")));

      String jmxDomain3 = JMX_DOMAIN + 3;
      GlobalConfigurationBuilder globalConfiguration3 = GlobalConfigurationBuilder.defaultClusteredBuilder();
      configureJmx(globalConfiguration3, jmxDomain3, mBeanServerLookup);
      cm3 = TestCacheManagerFactory.createClusteredCacheManager(globalConfiguration3, new ConfigurationBuilder());

      ConfigurationBuilder localCache3 = config();//local by default
      localCache3.statistics().enable();
      cm3.defineConfiguration("local_cache", localCache.build());
      cm3.getCache("local_cache");
      assertTrue(server.isRegistered(getCacheObjectName(jmxDomain3, "local_cache(local)", "Statistics")));
   }

   public void testUnregisterJmxInfoOnStop() {
      GlobalConfigurationBuilder globalConfiguration = GlobalConfigurationBuilder.defaultClusteredBuilder();
      configureJmx(globalConfiguration, JMX_DOMAIN, mBeanServerLookup);
      cm = TestCacheManagerFactory.createClusteredCacheManager(globalConfiguration, new ConfigurationBuilder());
      String jmxDomain = cm.getCacheManagerConfiguration().jmx().domain();
      ConfigurationBuilder localCache = config();//local by default
      localCache.statistics().enable();
      cm.defineConfiguration("local_cache", localCache.build());
      cm.getCache("local_cache");
      assertTrue(server.isRegistered(getCacheObjectName(jmxDomain, "local_cache(local)", "Statistics")));

      TestingUtil.killCacheManagers(cm);

      assertFalse(server.isRegistered(getCacheObjectName(jmxDomain, "local_cache(local)", "Statistics")));
      assertFalse(existsDomain(server, jmxDomain));
   }

   public void testCorrectUnregistering() {
      assertFalse(existsDomain(server, "infinispan"));
      GlobalConfigurationBuilder globalConfiguration = new GlobalConfigurationBuilder();
      configureJmx(globalConfiguration, JMX_DOMAIN, mBeanServerLookup);
      cm = TestCacheManagerFactory.createCacheManager(globalConfiguration, new ConfigurationBuilder());
      ConfigurationBuilder localCache = config();//local by default
      cm.defineConfiguration("local_cache", localCache.build());
      cm.getCache("local_cache");
      String jmxDomain = cm.getCacheManagerConfiguration().jmx().domain();
      assertTrue(server.isRegistered(getCacheObjectName(jmxDomain, "local_cache(local)", "Statistics")));
      assertTrue(server.isRegistered(getCacheObjectName(jmxDomain, "local_cache(local)", "Cache")));

      //now register a global one
      GlobalConfigurationBuilder globalConfiguration2 = GlobalConfigurationBuilder.defaultClusteredBuilder();
      configureJmx(globalConfiguration2, JMX_DOMAIN + 2, mBeanServerLookup);
      cm2 = TestCacheManagerFactory.createClusteredCacheManager(globalConfiguration2, new ConfigurationBuilder());
      ConfigurationBuilder remoteCache = new ConfigurationBuilder();
      remoteCache.statistics().enable();
      remoteCache.clustering().cacheMode(CacheMode.REPL_SYNC);
      cm2.defineConfiguration("remote_cache", remoteCache.build());
      cm2.getCache("remote_cache");
      String jmxDomain2 = cm2.getCacheManagerConfiguration().jmx().domain();
      assertTrue(server.isRegistered(getCacheObjectName(jmxDomain2, "remote_cache(repl_sync)", "Cache")));
      assertTrue(server.isRegistered(getCacheObjectName(jmxDomain2, "remote_cache(repl_sync)", "Statistics")));

      cm2.stop();
      assertTrue(server.isRegistered(getCacheObjectName(jmxDomain, "local_cache(local)", "Statistics")));
      assertFalse(server.isRegistered(getCacheObjectName(jmxDomain2, "remote_cache(repl_sync)", "CacheComponent")));
      assertFalse(server.isRegistered(getCacheObjectName(jmxDomain2, "remote_cache(repl_sync)", "Statistics")));

      cm.stop();
      assertFalse(server.isRegistered(getCacheObjectName(jmxDomain, "local_cache(local)", "Statistics")));
      assertFalse(server.isRegistered(getCacheObjectName(jmxDomain2, "remote_cache(repl_sync)", "Statistics")));
   }

   public void testStopUnstartedCacheManager() {
      GlobalConfigurationBuilder globalConfiguration = new GlobalConfigurationBuilder();
      configureJmx(globalConfiguration, JMX_DOMAIN, mBeanServerLookup);
      cm = TestCacheManagerFactory.createCacheManager(globalConfiguration, new ConfigurationBuilder());
      cm.stop();
   }

   public void testConfigurationProperties() throws Exception {
      GlobalConfigurationBuilder globalConfiguration = GlobalConfigurationBuilder.defaultClusteredBuilder();
      globalConfiguration.transport().siteId("TESTVALUE1");
      globalConfiguration.transport().rackId("TESTVALUE2");
      globalConfiguration.transport().machineId("TESTVALUE3");
      configureJmx(globalConfiguration, JMX_DOMAIN, mBeanServerLookup);
      cm = TestCacheManagerFactory.createClusteredCacheManager(globalConfiguration, new ConfigurationBuilder());
      String jmxDomain = cm.getCacheManagerConfiguration().jmx().domain();

      ConfigurationBuilder localCache = config();
      localCache.memory().storage(StorageType.HEAP);
      cm.defineConfiguration("local_cache1", localCache.build());
      localCache.memory().storage(StorageType.OFF_HEAP);
      cm.defineConfiguration("local_cache2", localCache.build());

      cm.getCache("local_cache1");
      cm.getCache("local_cache2");

      Properties props1 = (Properties) server.getAttribute(getCacheObjectName(jmxDomain, "local_cache1(local)", "Cache"), "configurationAsProperties");
      Properties props2 = (Properties) server.getAttribute(getCacheObjectName(jmxDomain, "local_cache2(local)", "Cache"), "configurationAsProperties");
      Properties propsGlobal = (Properties) server.getAttribute(getCacheManagerObjectName(jmxDomain), "globalConfigurationAsProperties");
      // configurationAsProperties excludes deprecated methods from the reflection, so 'storageType' is not available anymore.
      assertEquals("HEAP", props1.getProperty("memory.storage"));
      assertEquals("OFF_HEAP", props2.getProperty("memory.storage"));
      log.tracef("propsGlobal=%s", propsGlobal);
      assertEquals("TESTVALUE1", propsGlobal.getProperty("transport.siteId"));
      assertEquals("TESTVALUE2", propsGlobal.getProperty("transport.rackId"));
      assertEquals("TESTVALUE3", propsGlobal.getProperty("transport.machineId"));
   }

   private ConfigurationBuilder config() {
      ConfigurationBuilder configuration = new ConfigurationBuilder();
      configuration.clustering().stateTransfer().fetchInMemoryState(false).statistics().enable();
      return configuration;
   }
}
