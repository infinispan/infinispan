/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.jmx;

import org.infinispan.config.Configuration;
import org.infinispan.config.GlobalConfiguration;
import org.infinispan.manager.CacheContainer;
import org.infinispan.manager.CacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

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
@Test(groups = "functional", testName = "jmx.JmxStatsFunctionalTest")
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
      GlobalConfiguration globalConfiguration = GlobalConfiguration.getClusteredDefault();
      globalConfiguration.setMBeanServerLookup(PerThreadMBeanServerLookup.class.getName());
      cm = TestCacheManagerFactory.createCacheManager(globalConfiguration);
      String jmxDomain = cm.getGlobalConfiguration().getJmxDomain();

      Configuration localCache = config();//local by default
      cm.defineConfiguration("local_cache", localCache);
      cm.getCache("local_cache");

      assert existsObject(getCacheObjectName(jmxDomain, "local_cache(local)", "Statistics"));
   }


   public void testOnlyGlobalJmxStatsEnabled() throws Exception {
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

      assert existsObject(getCacheManagerObjectName(jmxDomain));

      assert !existsObject(getCacheObjectName(jmxDomain, "local_cache(local)", "Statistics"));
      assert !existsObject(getCacheObjectName(jmxDomain, "remote1(repl_sync)", "Statistics"));

      // Since ISPN-2290
      assert existsObject(getCacheObjectName(jmxDomain, "remote1(repl_sync)", "LockManager"));
      assert existsObject(getCacheObjectName(jmxDomain, "local_cache(local)", "LockManager"));


   }

   public void testOnlyPerCacheJmxStatsEnabled() throws Exception {
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

      assert existsObject(getCacheObjectName(jmxDomain, "local_cache(local)", "Statistics"));
      // Since ISPN-2290
      assert existsObject(getCacheManagerObjectName(jmxDomain));
      assert existsObject(getCacheObjectName(jmxDomain, "remote1(repl_sync)", "RpcManager"));
   }

   public void testMultipleManagersOnSameServerFails(Method method) throws Exception {
      final String jmxDomain = JMX_DOMAIN + '.' + method.getName();
      GlobalConfiguration globalConfiguration = GlobalConfiguration.getClusteredDefault();
      globalConfiguration.setJmxDomain(jmxDomain);
      globalConfiguration.setExposeGlobalJmxStatistics(true);
      globalConfiguration.setMBeanServerLookup(PerThreadMBeanServerLookup.class.getName());
      cm = TestCacheManagerFactory.createCacheManagerEnforceJmxDomain(globalConfiguration);
      Configuration localCache = config();//local by default
      localCache.setExposeJmxStatistics(true);
      cm.defineConfiguration("local_cache", localCache);
      cm.getCache("local_cache");
      assert existsObject(getCacheObjectName(jmxDomain, "local_cache(local)", "Statistics"));

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
         ObjectName duplicateObjectName = getCacheManagerObjectName(duplicateName);
         server.getAttribute(duplicateObjectName, "CreatedCacheCount").equals("0");
      } finally {
         duplicateAllowedContainer.stop();
      }
   }

   public void testMultipleManagersOnSameServerWithCloneFails() throws Exception {
      GlobalConfiguration globalConfiguration = GlobalConfiguration.getClusteredDefault();
      globalConfiguration.setMBeanServerLookup(PerThreadMBeanServerLookup.class.getName());
      globalConfiguration.setExposeGlobalJmxStatistics(true);
      cm = TestCacheManagerFactory.createCacheManager(globalConfiguration);
      Configuration localCache = config();//local by default
      localCache.setExposeJmxStatistics(true);
      cm.defineConfiguration("local_cache", localCache);
      cm.getCache("local_cache");
      String jmxDomain = globalConfiguration.getJmxDomain();
      assert existsObject(getCacheObjectName(jmxDomain, "local_cache(local)", "Statistics"));

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

   public void testMultipleManagersOnSameServer() throws Exception {
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
      assert existsObject(getCacheObjectName(jmxDomain, "local_cache(local)", "Statistics"));

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
      assert existsObject(getCacheObjectName(jmxDomain2, "local_cache(local)", "Statistics"));

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
      assert existsObject(getCacheObjectName(jmxDomain3, "local_cache(local)", "Statistics"));
   }

   public void testUnregisterJmxInfoOnStop() throws Exception {
      GlobalConfiguration globalConfiguration = GlobalConfiguration.getClusteredDefault();
      globalConfiguration.setExposeGlobalJmxStatistics(true);
      globalConfiguration.setMBeanServerLookup(PerThreadMBeanServerLookup.class.getName());
      cm = TestCacheManagerFactory.createCacheManager(globalConfiguration);
      Configuration localCache = config();//local by default
      localCache.setExposeJmxStatistics(true);
      cm.defineConfiguration("local_cache", localCache);
      cm.getCache("local_cache");
      String jmxDomain = globalConfiguration.getJmxDomain();
      assert existsObject(getCacheObjectName(jmxDomain, "local_cache(local)", "Statistics"));

      TestingUtil.killCacheManagers(cm);

      assert !existsObject(getCacheObjectName(jmxDomain, "local_cache(local)", "Statistics"));
      assert !existsDomains(jmxDomain);
   }

   public void testCorrectUnregistering() throws Exception {
      assert !existsDomains("infinispan");
      GlobalConfiguration globalConfiguration = GlobalConfiguration.getNonClusteredDefault();
      globalConfiguration.setMBeanServerLookup(PerThreadMBeanServerLookup.class.getName());
      cm = TestCacheManagerFactory.createCacheManager(globalConfiguration);
      Configuration localCache = config();//local by default
      cm.defineConfiguration("local_cache", localCache);
      cm.getCache("local_cache");
      String jmxDomain = cm.getGlobalConfiguration().getJmxDomain();
      assert existsObject(getCacheObjectName(jmxDomain, "local_cache(local)", "Statistics"));
      assert existsObject(getCacheObjectName(jmxDomain, "local_cache(local)", "Cache"));

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
      GlobalConfiguration globalConfiguration = GlobalConfiguration.getNonClusteredDefault();
      globalConfiguration.setExposeGlobalJmxStatistics(true);
      globalConfiguration.setMBeanServerLookup(PerThreadMBeanServerLookup.class.getName());
      cm = TestCacheManagerFactory.createCacheManager(false, globalConfiguration);
      cm.stop();
   }

   private Configuration config() {
      Configuration configuration = new Configuration();
      configuration.setFetchInMemoryState(false);
      configuration.setExposeJmxStatistics(true);
      return configuration;
   }
}
