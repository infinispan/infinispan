/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other
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
import org.infinispan.api.WithClassLoaderTest;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.marshall.jboss.DefaultContextClassResolver;
import org.infinispan.test.CherryPickClassLoader;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import static org.infinispan.test.fwk.TestCacheManagerFactory.createClusteredCacheManager;

/**
 * Tests behaviour of cache restarts when a custom class resolver is configured.
 *
 * @author Galder Zamarreño
 * @since 5.1
 */
@Test(groups = "functional", testName = "marshall.CustomClassResolverCacheRestartTest")
public class CustomClassResolverCacheRestartTest extends MultipleCacheManagersTest {

   private static final String BASE = CustomClassResolverCacheRestartTest.class.getName() + "$";

   private static final String CAR = BASE + "Car";

   private ClassLoader systemCl;
   private ClassLoader cherryPickCl;

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.storeAsBinary().enable()
            .clustering().cacheMode(CacheMode.REPL_SYNC);
      EmbeddedCacheManager cm0 = createClusteredCacheManager(builder);
      cacheManagers.add(cm0);

      String[] notFound = new String[]{CAR, "org.infinispan.util.ImmutableListCopy"};
      systemCl = Thread.currentThread().getContextClassLoader();
      cherryPickCl = new CherryPickClassLoader(null, null, notFound, systemCl);

      GlobalConfigurationBuilder gcBuilder = createSecondGlobalCfgBuilder(cherryPickCl);
      EmbeddedCacheManager cm1 = createClusteredCacheManager(gcBuilder, builder);
      cacheManagers.add(cm1);
   }

   @AfterClass(alwaysRun = true)
   protected void destroy() {
      super.destroy();
      systemCl = null;
      cherryPickCl = null;
   }

   protected GlobalConfigurationBuilder createSecondGlobalCfgBuilder(ClassLoader cl) {
      GlobalConfigurationBuilder gcBuilder =
            GlobalConfigurationBuilder.defaultClusteredBuilder();
      gcBuilder.serialization().classResolver(new DefaultContextClassResolver(cl));
      return gcBuilder;
   }

   public void testCacheRestart() throws Exception {
      final Cache<Integer, WithClassLoaderTest.Car> cache0 = cache(0);
      final Cache<Integer, WithClassLoaderTest.Car> cache1 = cache(1);

      WithClassLoaderTest.Car value = new
            WithClassLoaderTest.Car().plateNumber("1234");
      cache0.put(1, value);

      // Restart the cache
      cache1.stop();
      cache1.start();
   }

}
