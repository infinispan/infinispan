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

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.loaders.dummy.DummyInMemoryCacheStoreConfigurationBuilder;
import org.infinispan.test.fwk.TestInternalCacheEntryFactory;
import org.infinispan.loaders.CacheLoaderManager;
import org.infinispan.loaders.CacheStore;
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

   private static final String JMX_DOMAIN = ActivationAndPassivationInterceptorMBeanTest.class.getSimpleName();

   CacheStore cacheStore;
   MBeanServer threadMBeanServer;
   final ObjectName activationInterceptorObjName =
         getCacheObjectName(JMX_DOMAIN, "___defaultcache(local)", "Activation");
   final ObjectName passivationInterceptorObjName =
         getCacheObjectName(JMX_DOMAIN, "___defaultcache(local)", "Passivation");

   protected EmbeddedCacheManager createCacheManager() throws Exception {
      GlobalConfigurationBuilder globalBuilder = new GlobalConfigurationBuilder();
      globalBuilder.globalJmxStatistics()
            .mBeanServerLookup(new PerThreadMBeanServerLookup())
            .jmxDomain(JMX_DOMAIN)
            .enable();

      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.eviction().strategy(EvictionStrategy.LRU).maxEntries(1)
            .jmxStatistics().enable()
            .loaders()
               .passivation(true)
               .addLoader(DummyInMemoryCacheStoreConfigurationBuilder.class);

      // Do not initiliaze this in instance declaration since a different
      // thread will be used when running from maven, breaking the thread local
      threadMBeanServer = PerThreadMBeanServerLookup.getThreadMBeanServer();

      return TestCacheManagerFactory.createCacheManager(globalBuilder, builder, true);
   }

   @Override
   protected void setup() throws Exception {
      super.setup();
      cacheStore = extractCacheStore();
   }

   @AfterMethod
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

      Map<String, String> toAdd = new HashMap<String, String>();
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
      assert Integer.valueOf(threadMBeanServer.getAttribute(
            activationInterceptorObjName, "Activations").toString())
            .equals(activationCount);
   }

   private void assertPassivationCount(int activationCount) throws Exception {
      Object passivations = threadMBeanServer.getAttribute(
            passivationInterceptorObjName, "Passivations");
      assertEquals(activationCount,
            Integer.valueOf(passivations.toString()).intValue());
   }

   private CacheStore extractCacheStore() {
      return TestingUtil.extractComponent(cache,
            CacheLoaderManager.class).getCacheStore();
   }

}
