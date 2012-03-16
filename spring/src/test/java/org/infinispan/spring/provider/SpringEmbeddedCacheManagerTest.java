/**
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
 *   ~
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

package org.infinispan.spring.provider;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertSame;
import static org.testng.AssertJUnit.assertTrue;

import java.io.IOException;
import java.util.Collection;

import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.springframework.cache.Cache;
import org.testng.annotations.Test;

/**
 * <p>
 * Test {@link SpringEmbeddedCacheManager}.
 * </p>
 * 
 * @author <a href="mailto:olaf DOT bergner AT gmx DOT de">Olaf Bergner</a>
 * @author Marius Bogoevici
 * 
 */
@Test(testName = "spring.provider.SpringEmbeddedCacheManagerTest", groups = "unit")
public class SpringEmbeddedCacheManagerTest {

   private static final String CACHE_NAME_FROM_CONFIGURATION_FILE = "asyncCache";

   private static final String NAMED_ASYNC_CACHE_CONFIG_LOCATION = "named-async-cache.xml";

   /**
    * Test method for
    * {@link org.infinispan.spring.provider.SpringEmbeddedCacheManager#SpringEmbeddedCacheManager(org.infinispan.manager.EmbeddedCacheManager)}
    * .
    */
   @Test(expectedExceptions = IllegalArgumentException.class)
   public final void springEmbeddedCacheManagerConstructorShouldRejectNullEmbeddedCacheManager() {
      new SpringEmbeddedCacheManager(null);
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.provider.SpringEmbeddedCacheManager#getCache(String)}.
    * 
    * @throws IOException
    */
   @Test
   public final void getCacheShouldReturnTheCacheHavingTheProvidedName() throws IOException {
      final EmbeddedCacheManager nativeCacheManager = new DefaultCacheManager(
               SpringEmbeddedCacheManagerTest.class
                        .getResourceAsStream(NAMED_ASYNC_CACHE_CONFIG_LOCATION));
      final SpringEmbeddedCacheManager objectUnderTest = new SpringEmbeddedCacheManager(
               nativeCacheManager);

      final Cache cacheExpectedToHaveTheProvidedName = objectUnderTest
               .getCache(CACHE_NAME_FROM_CONFIGURATION_FILE);

      assertEquals(
               "getCache("
                        + CACHE_NAME_FROM_CONFIGURATION_FILE
                        + ") should have returned the cache having the provided name. However, the cache returned has a different name.",
               CACHE_NAME_FROM_CONFIGURATION_FILE, cacheExpectedToHaveTheProvidedName.getName());
      nativeCacheManager.stop();
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.provider.SpringEmbeddedCacheManager#getCache(String)}.
    * 
    * @throws IOException
    */
   @Test
   public final void getCacheShouldReturnACacheAddedAfterCreatingTheSpringEmbeddedCache()
            throws IOException {
      final String nameOfInfinispanCacheAddedLater = "infinispan.cache.addedLater";

      final EmbeddedCacheManager nativeCacheManager = new DefaultCacheManager(
               SpringEmbeddedCacheManagerTest.class
                        .getResourceAsStream(NAMED_ASYNC_CACHE_CONFIG_LOCATION));
      final SpringEmbeddedCacheManager objectUnderTest = new SpringEmbeddedCacheManager(
               nativeCacheManager);

      final org.infinispan.Cache<Object, Object> infinispanCacheAddedLater = nativeCacheManager
               .getCache(nameOfInfinispanCacheAddedLater);

      final Cache springCacheAddedLater = objectUnderTest
               .getCache(nameOfInfinispanCacheAddedLater);

      assertEquals(
               "getCache("
                        + nameOfInfinispanCacheAddedLater
                        + ") should have returned the Spring cache having the Infinispan cache added after creating "
                        + "SpringEmbeddedCacheManager as its underlying native cache. However, the underlying native cache is different.",
               infinispanCacheAddedLater, springCacheAddedLater.getNativeCache());
      nativeCacheManager.stop();
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.provider.SpringEmbeddedCacheManager#getCacheNames()}.
    * 
    * @throws IOException
    */
   @Test
   public final void getCacheNamesShouldReturnAllCachesDefinedInConfigurationFile()
            throws IOException {
      final EmbeddedCacheManager nativeCacheManager = new DefaultCacheManager(
               SpringEmbeddedCacheManagerTest.class
                        .getResourceAsStream(NAMED_ASYNC_CACHE_CONFIG_LOCATION));
      final SpringEmbeddedCacheManager objectUnderTest = new SpringEmbeddedCacheManager(
               nativeCacheManager);

      final Collection<String> cacheNames = objectUnderTest.getCacheNames();

      assertTrue(
               "SpringEmbeddedCacheManager should load all named caches found in the configuration file of the wrapped "
                        + "native cache manager. However, it does not know about the cache named "
                        + CACHE_NAME_FROM_CONFIGURATION_FILE
                        + " defined in said configuration file.",
               cacheNames.contains(CACHE_NAME_FROM_CONFIGURATION_FILE));
      nativeCacheManager.stop();
   }

   /**
    * Test method for {@link org.infinispan.spring.provider.SpringEmbeddedCacheManager#stop()}.
    * 
    * @throws IOException
    */
   @Test
   public final void stopShouldStopTheNativeEmbeddedCacheManager() throws IOException {
      final EmbeddedCacheManager nativeCacheManager = new DefaultCacheManager();
      nativeCacheManager.getCache(); // Implicitly starts EmbeddedCacheManager
      final SpringEmbeddedCacheManager objectUnderTest = new SpringEmbeddedCacheManager(
               nativeCacheManager);

      objectUnderTest.stop();

      assertEquals("Calling stop() on SpringEmbeddedCacheManager should stop the enclosed "
               + "Infinispan EmbeddedCacheManager. However, it is still running.",
               ComponentStatus.TERMINATED, nativeCacheManager.getStatus());
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.provider.SpringEmbeddedCacheManager#getNativeCache()}.
    * 
    * @throws IOException
    */
   @Test
   public final void getNativeCacheShouldReturnTheEmbeddedCacheManagerSuppliedAtConstructionTime()
            throws IOException {
      final EmbeddedCacheManager nativeCacheManager = new DefaultCacheManager();
      final SpringEmbeddedCacheManager objectUnderTest = new SpringEmbeddedCacheManager(
               nativeCacheManager);

      final EmbeddedCacheManager nativeCacheManagerReturned = objectUnderTest
               .getNativeCacheManager();

      assertSame(
               "getNativeCacheManager() should have returned the EmbeddedCacheManager supplied at construction time. However, it retuned a different one.",
               nativeCacheManager, nativeCacheManagerReturned);
      nativeCacheManager.stop();
   }
}
