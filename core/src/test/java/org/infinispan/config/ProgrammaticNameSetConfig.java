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

package org.infinispan.config;

import org.infinispan.Cache;
import org.infinispan.manager.CacheContainer;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

/**
 * @author Mircea Markus
 * @since 4.2
 */
@Test (groups = "functional", testName = "config.ProgrammaticNameSetConfig")
public class ProgrammaticNameSetConfig extends SingleCacheManagerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      return TestCacheManagerFactory.createCacheManager(getDefaultStandaloneConfig(true));
   }

   public void testGetNotDefaultCache() {
      Configuration configurationOverride = new Configuration();
      configurationOverride.fluent().mode(Configuration.CacheMode.LOCAL);
      String aName = "aName";
      Configuration configuration = cacheManager.defineConfiguration(aName, configurationOverride);
      Cache c = cacheManager.getCache(aName);
      assertEquals(c.getConfiguration().getName(), aName);
      assertEquals(configuration.getName(), aName);
   }

   public void testGetNameForDefaultCache() {
      String name = cacheManager.getCache().getConfiguration().getName();
      assertEquals(name, CacheContainer.DEFAULT_CACHE_NAME);
   }

   public void getNameForUndefinedCache() {
      Configuration configuration = cacheManager.getCache("undefinedCache").getConfiguration();
      assertEquals(configuration.getName(), "undefinedCache");
   }
}
