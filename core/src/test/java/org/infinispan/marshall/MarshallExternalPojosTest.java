/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other
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

package org.infinispan.marshall;

import org.infinispan.Cache;
import org.infinispan.config.Configuration;
import org.infinispan.config.GlobalConfiguration;
import org.infinispan.manager.CacheContainer;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import java.lang.reflect.Method;

import static org.infinispan.test.TestingUtil.k;
import static org.testng.AssertJUnit.assertEquals;

@Test(groups = "functional", testName = "marshall.jboss.MarshallExternalPojosTest")
public class MarshallExternalPojosTest extends MultipleCacheManagersTest {

   private static final String CACHE_NAME = MarshallExternalPojosTest.class.getName();

   @Override
   protected void createCacheManagers() throws Throwable {
      GlobalConfiguration globalCfg1 = GlobalConfiguration.getClusteredDefault();
      GlobalConfiguration globalCfg2 = GlobalConfiguration.getClusteredDefault();
      CacheContainer cm1 = TestCacheManagerFactory.createCacheManager(globalCfg1);
      CacheContainer cm2 = TestCacheManagerFactory.createCacheManager(globalCfg2);
      registerCacheManager(cm1, cm2);
      Configuration cfg = getDefaultClusteredConfig(Configuration.CacheMode.REPL_SYNC);
      defineConfigurationOnAllManagers(CACHE_NAME, cfg);
      waitForClusterToForm(CACHE_NAME);
   }

   public void testReplicateJBossExternalizePojo(Method m) {
      PojoWithJBossExternalize pojo = new PojoWithJBossExternalize(34, k(m));
      doReplicatePojo(m, pojo);
   }

   @Test(dependsOnMethods = "testReplicateJBossExternalizePojo")
   public void testReplicateJBossExternalizePojoToNewJoiningNode(Method m) {
      PojoWithJBossExternalize pojo = new PojoWithJBossExternalize(48, k(m));
      doReplicatePojoToNewJoiningNode(m, pojo);
   }

   public void testReplicateMarshallableByPojo(Method m) {
      PojoWithSerializeWith pojo = new PojoWithSerializeWith(17, k(m));
      doReplicatePojo(m, pojo);
   }

   @Test(dependsOnMethods = "testReplicateMarshallableByPojo")
   public void testReplicateMarshallableByPojoToNewJoiningNode(Method m) {
      PojoWithSerializeWith pojo = new PojoWithSerializeWith(85, k(m));
      doReplicatePojoToNewJoiningNode(m, pojo);
   }

   private void doReplicatePojo(Method m, Object o) {
      Cache cache1 = manager(0).getCache(CACHE_NAME);
      Cache cache2 = manager(1).getCache(CACHE_NAME);
      cache1.put(k(m), o);
      assertEquals(o, cache2.get(k(m)));
   }

   private void doReplicatePojoToNewJoiningNode(Method m, Object o) {
      Cache cache1 = manager(0).getCache(CACHE_NAME);
      EmbeddedCacheManager cm = createCacheManager();
      try {
         Cache cache3 = cm.getCache(CACHE_NAME);
         cache1.put(k(m), o);
         assertEquals(o, cache3.get(k(m)));
      } finally {
         cm.stop();
      }
   }

   private EmbeddedCacheManager createCacheManager() {
      GlobalConfiguration globalCfg = GlobalConfiguration.getClusteredDefault();
      EmbeddedCacheManager cm = TestCacheManagerFactory.createCacheManager(globalCfg);
      Configuration cfg = getDefaultClusteredConfig(Configuration.CacheMode.REPL_SYNC);
      cm.defineConfiguration(CACHE_NAME, cfg);
      return cm;
   }
}
