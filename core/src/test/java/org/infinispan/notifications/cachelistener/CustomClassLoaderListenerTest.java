/*
 * JBoss, Home of Professional Open Source
 * Copyright 2013 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */
package org.infinispan.notifications.cachelistener;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.loaders.dummy.DummyInMemoryCacheStoreConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntriesEvicted;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryActivated;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryCreated;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryLoaded;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryModified;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryPassivated;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryRemoved;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryVisited;
import org.infinispan.notifications.cachelistener.event.Event;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.*;

/**
 * CustomClassLoaderListenerTest.
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
@Test(groups = "functional", testName = "notifications.cachelistener.CustomClassLoaderListenerTest")
public class CustomClassLoaderListenerTest extends SingleCacheManagerTest {

   private CustomClassLoader ccl;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder builder = getDefaultStandaloneCacheConfig(false);
      builder.loaders().passivation(true).addStore(DummyInMemoryCacheStoreConfigurationBuilder.class);

      return TestCacheManagerFactory.createCacheManager(builder);
   }

   public void testCustomClassLoaderListener() throws Exception {
      ccl = new CustomClassLoader(Thread.currentThread().getContextClassLoader());
      ClassLoaderListener listener = new ClassLoaderListener();
      cache().getAdvancedCache().with(ccl).addListener(listener);
      cache().put("a", "a"); // Created + Modified
      cache().replace("a", "b"); // Modified

      cache().evict("a"); // Passivated + Evicted

      cache().get("a"); // Loaded +  Activated + Visited

      cache().remove("a"); // Removed


      assertEquals(9, listener.invocationCount);
   }

   public static class CustomClassLoader extends ClassLoader {
      public CustomClassLoader(ClassLoader parent) {
         super(parent);
      }
   }

   @Listener
   public class ClassLoaderListener {
      int invocationCount = 0;

      @CacheEntryActivated
      @CacheEntryCreated
      @CacheEntriesEvicted
      @CacheEntryLoaded
      @CacheEntryModified
      @CacheEntryPassivated
      @CacheEntryRemoved
      @CacheEntryVisited
      public void handle(Event e) {
         assertEquals(ccl, Thread.currentThread().getContextClassLoader());
         if(!e.isPre()) {
            ++invocationCount;
         }
      }
   }
}
