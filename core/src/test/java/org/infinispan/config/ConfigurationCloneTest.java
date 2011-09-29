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
package org.infinispan.config;

import java.lang.reflect.Method;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "config.ConfigurationCloneTest")
public class ConfigurationCloneTest extends SingleCacheManagerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      return TestCacheManagerFactory.createLocalCacheManager(false);
   }

   public void testCloningBeforeStart(Method method) {
      Configuration defaultConfig = cacheManager.defineConfiguration("default", new Configuration());
      Configuration clone = defaultConfig.clone();
      assert clone.equals(defaultConfig);
      clone.setEvictionMaxEntries(Integer.MAX_VALUE);
      String name = method.getName() + "-default";
      cacheManager.defineConfiguration(name, clone);
      cacheManager.getCache(name);
   }

   public void testCloningAfterStart(Method method) {
      Configuration defaultConfig = cacheManager.getCache("default").getConfiguration();
      Configuration clone = defaultConfig.clone();
      assert clone.equals(defaultConfig);
      clone.setEvictionMaxEntries(Integer.MAX_VALUE);
      String name = method.getName() + "-default";
      cacheManager.defineConfiguration(name, clone);
      cacheManager.getCache(name);
   }
   
   public void testDoubleCloning(Method method) {
      String name = method.getName();
      Configuration defaultConfig = cacheManager.defineConfiguration(name + "-default", new Configuration());
      Configuration clone = defaultConfig.clone();
      assert clone.equals(defaultConfig);
      clone.setEvictionMaxEntries(Integer.MAX_VALUE);
      cacheManager.defineConfiguration(name + "-new-default", clone);
      cacheManager.getCache(name + "-new-default");

      Configuration otherDefaultConfig = cacheManager.getCache(name + "-default").getConfiguration();
      Configuration otherClone = otherDefaultConfig.clone();
      assert otherClone.equals(otherDefaultConfig);
      otherClone.setEvictionMaxEntries(Integer.MAX_VALUE - 1);
      
      try {
         cacheManager.defineConfiguration(name + "-new-default", otherClone);
      } catch (ConfigurationException e) {
         String message = e.getMessage();
         assert message.contains("[maxEntries]") : "Exception should indicate that it's Eviction maxEntries that we're trying to override but it says: " + message;
      }
   }

   public void testGlobalConfigurationCloning(Method m) {
      GlobalConfiguration clone = cacheManager.getGlobalConfiguration().clone();
      String newJmxDomain = m.getName();
      clone.setJmxDomain(newJmxDomain);
      EmbeddedCacheManager cacheManager = TestCacheManagerFactory.createCacheManagerEnforceJmxDomain(clone);
      assert cacheManager.getGlobalConfiguration().getJmxDomain().equals(newJmxDomain);
   }
}
