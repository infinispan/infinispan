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
package org.infinispan.loaders;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.loaders.dummy.DummyInMemoryCacheStore;
import org.infinispan.loaders.dummy.DummyInMemoryCacheStoreConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;
import static org.testng.AssertJUnit.*;

/**
 * ClassLoaderManagerDisablingTest.
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
@Test(groups="functional", testName="loaders.ClassLoaderManagerDisablingTest")
public class ClassLoaderManagerDisablingTest extends AbstractInfinispanTest {

   public void testStoreDisabling() {
      ConfigurationBuilder builder = TestCacheManagerFactory.getDefaultCacheConfiguration(false);
      builder.loaders().addStore(DummyInMemoryCacheStoreConfigurationBuilder.class);
      EmbeddedCacheManager cm = TestCacheManagerFactory.createCacheManager(builder);
      try {
         checkAndDisableStore(cm);
      } finally {
         TestingUtil.killCacheManagers(cm);
      }
   }

   public void testAsyncStoreDisabling() {
      ConfigurationBuilder builder = TestCacheManagerFactory.getDefaultCacheConfiguration(false);
      builder.loaders().addStore(DummyInMemoryCacheStoreConfigurationBuilder.class).async().enable();
      EmbeddedCacheManager cm = TestCacheManagerFactory.createCacheManager(builder);
      try {
         checkAndDisableStore(cm);
      } finally {
         TestingUtil.killCacheManagers(cm);
      }
   }

   public void testSingletonStoreDisabling() {
      ConfigurationBuilder builder = TestCacheManagerFactory.getDefaultCacheConfiguration(false);
      builder.loaders().addStore(DummyInMemoryCacheStoreConfigurationBuilder.class).singletonStore().enable();
      EmbeddedCacheManager cm = TestCacheManagerFactory.createCacheManager(builder);
      try {
         checkAndDisableStore(cm);
      } finally {
         TestingUtil.killCacheManagers(cm);
      }
   }

   public void testChainingStoreDisabling() {
      ConfigurationBuilder builder = TestCacheManagerFactory.getDefaultCacheConfiguration(false);
      builder.loaders().addStore(DummyInMemoryCacheStoreConfigurationBuilder.class).loaders().addStore(DummyInMemoryCacheStoreConfigurationBuilder.class).async().enable();
      EmbeddedCacheManager cm = TestCacheManagerFactory.createCacheManager(builder);
      try {
         checkAndDisableStore(cm, 2);
      } finally {
         TestingUtil.killCacheManagers(cm);
      }
   }

   private void checkAndDisableStore(EmbeddedCacheManager cm) {
      checkAndDisableStore(cm, 1);
   }

   private void checkAndDisableStore(EmbeddedCacheManager cm, int count) {
      Cache<Object, Object> cache = cm.getCache();
      CacheLoaderManager clm = TestingUtil.extractComponent(cache, CacheLoaderManager.class);
      assertTrue(clm.isEnabled());
      assertEquals(count, clm.getCacheLoaders(DummyInMemoryCacheStore.class).size());
      clm.disableCacheStore(DummyInMemoryCacheStore.class.getName());
      assertFalse(clm.isEnabled());
   }
}
