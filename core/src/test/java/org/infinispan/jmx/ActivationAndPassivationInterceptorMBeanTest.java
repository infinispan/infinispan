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

import org.infinispan.Cache;
import org.infinispan.config.CacheLoaderManagerConfig;
import org.infinispan.config.Configuration;
import org.infinispan.config.GlobalConfiguration;
import org.infinispan.test.fwk.TestInternalCacheEntryFactory;
import org.infinispan.loaders.CacheLoaderManager;
import org.infinispan.loaders.CacheStore;
import org.infinispan.loaders.dummy.DummyInMemoryCacheStore;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import static org.infinispan.test.TestingUtil.*;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import javax.management.Attribute;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import static org.testng.AssertJUnit.assertEquals;

/**
 * Tester class for ActivationInterceptor and PassivationInterceptor.
 *
 * @author Mircea.Markus@jboss.com
 * @author Galder Zamarreño
 */
@Test(groups = "functional", testName = "jmx.ActivationAndPassivationInterceptorMBeanTest")
public class ActivationAndPassivationInterceptorMBeanTest extends SingleCacheManagerTest {
   Cache cache;
   MBeanServer threadMBeanServer;
   ObjectName activationInterceptorObjName;
   ObjectName passivationInterceptorObjName;
   CacheStore cacheStore;
   private static final String JMX_DOMAIN = ActivationAndPassivationInterceptorMBeanTest.class.getSimpleName();

   protected EmbeddedCacheManager createCacheManager() throws Exception {
      GlobalConfiguration globalConfiguration = GlobalConfiguration.getNonClusteredDefault();
      globalConfiguration.setMBeanServerLookup(PerThreadMBeanServerLookup.class.getName());
      globalConfiguration.setJmxDomain(JMX_DOMAIN);
      globalConfiguration.setExposeGlobalJmxStatistics(true);
      cacheManager = TestCacheManagerFactory.createCacheManagerEnforceJmxDomain(globalConfiguration);
      DummyInMemoryCacheStore.Cfg cfg = new DummyInMemoryCacheStore.Cfg();
      CacheLoaderManagerConfig clManagerConfig = new CacheLoaderManagerConfig();
      clManagerConfig.setPassivation(true);
      clManagerConfig.addCacheLoaderConfig(cfg);
      Configuration configuration = getDefaultClusteredConfig(Configuration.CacheMode.LOCAL);
      configuration.setExposeJmxStatistics(true);
      configuration.setCacheLoaderManagerConfig(clManagerConfig);

      cacheManager.defineConfiguration("test", configuration);
      cache = cacheManager.getCache("test");
      activationInterceptorObjName = getCacheObjectName(JMX_DOMAIN, "test(local)", "Activation");
      passivationInterceptorObjName = getCacheObjectName(JMX_DOMAIN, "test(local)", "Passivation");

      threadMBeanServer = PerThreadMBeanServerLookup.getThreadMBeanServer();
      cacheStore = TestingUtil.extractComponent(cache, CacheLoaderManager.class).getCacheStore();

      return cacheManager;
   }

   @AfterMethod(alwaysRun = true)
   public void resetStats() throws Exception {
      threadMBeanServer.invoke(activationInterceptorObjName, "resetStatistics", new Object[0], new String[0]);
   }

   public void testDisableStatistics() throws Exception {
      threadMBeanServer.setAttribute(activationInterceptorObjName, new Attribute("StatisticsEnabled", Boolean.FALSE));
      assert threadMBeanServer.getAttribute(activationInterceptorObjName, "Activations").toString().equals("N/A");
      threadMBeanServer.setAttribute(activationInterceptorObjName, new Attribute("StatisticsEnabled", Boolean.TRUE));
   }

   public void testActivationOnGet(Method m) throws Exception {
      assertActivationCount(0);
      assert cache.get(k(m)) == null;
      assertActivationCount(0);
      cacheStore.store(TestInternalCacheEntryFactory.create(k(m), v(m)));
      assert cacheStore.containsKey(k(m));
      assert cache.get(k(m)).equals(v(m));
      assertActivationCount(1);
      assert !cacheStore.containsKey(k(m));
   }

   public void testActivationOnPut(Method m) throws Exception {
      assertActivationCount(0);
      assert cache.get(k(m)) == null;
      assertActivationCount(0);
      cacheStore.store(TestInternalCacheEntryFactory.create(k(m), v(m)));
      assert cacheStore.containsKey(k(m));
      cache.put(k(m), v(m, 2));
      assert cache.get(k(m)).equals(v(m, 2));
      assertActivationCount(1);
      assert !cacheStore.containsKey(k(m)) : "this should only be persisted on evict";
   }

   public void testActivationOnRemove(Method m) throws Exception {
      assertActivationCount(0);
      assert cache.get(k(m)) == null;
      assertActivationCount(0);
      cacheStore.store(TestInternalCacheEntryFactory.create(k(m), v(m)));
      assert cacheStore.containsKey(k(m));
      assert cache.remove(k(m)).equals(v(m));
      assertActivationCount(1);
      assert !cacheStore.containsKey(k(m));
   }

   public void testActivationOnReplace(Method m) throws Exception {
      assertActivationCount(0);
      assert cache.get(k(m)) == null;
      assertActivationCount(0);
      cacheStore.store(TestInternalCacheEntryFactory.create(k(m), v(m)));
      assert cacheStore.containsKey(k(m));
      assert cache.replace(k(m), v(m, 2)).equals(v(m));
      assertActivationCount(1);
      assert !cacheStore.containsKey(k(m));
   }

   public void testActivationOnPutMap(Method m) throws Exception {
      assertActivationCount(0);
      assert cache.get(k(m)) == null;
      assertActivationCount(0);
      cacheStore.store(TestInternalCacheEntryFactory.create(k(m), v(m)));
      assert cacheStore.containsKey(k(m));

      Map toAdd = new HashMap();
      toAdd.put(k(m), v(m, 2));
      cache.putAll(toAdd);
      assertActivationCount(1);
      assert cache.get(k(m)).equals(v(m, 2));
      assert !cacheStore.containsKey(k(m));
   }

   public void testPassivationOnEvict(Method m) throws Exception {
      assertPassivationCount(0);
      cache.put(k(m), v(m));
      cache.put(k(m, 2), v(m, 2));
      cache.evict(k(m));
      assertPassivationCount(1);
      cache.evict(k(m, 2));
      assertPassivationCount(2);
      cache.evict("not_existing_key");
      assertPassivationCount(2);
   }

   private void assertActivationCount(int activationCount) throws Exception {
      assert Integer.valueOf(threadMBeanServer.getAttribute(activationInterceptorObjName, "Activations").toString()).equals(activationCount);
   }

   private void assertPassivationCount(int activationCount) throws Exception {
      Object passivations = threadMBeanServer.getAttribute(passivationInterceptorObjName, "Passivations");
      assertEquals(activationCount, Integer.valueOf(passivations.toString()).intValue());
   }

}
