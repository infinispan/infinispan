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
package org.infinispan.loaders;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.concurrent.ExecutorService;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.util.DefaultTimeService;
import org.infinispan.util.ReflectionUtil;
import org.infinispan.util.concurrent.WithinThreadExecutor;
import org.mockito.Mockito;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Unit tests that cover {@link  AbstractCacheStoreTest }
 *
 * @author Adrian Cole
 * @since 4.0
 */
@Test(groups = "unit", testName = "loaders.AbstractCacheStoreTest")
public class AbstractCacheStoreTest extends AbstractInfinispanTest {
   private AbstractCacheStore cs;
   private AbstractCacheStoreConfig cfg;

   @BeforeMethod
   public void setUp() throws NoSuchMethodException, CacheLoaderException {
      cs = mock(AbstractCacheStore.class, Mockito.CALLS_REAL_METHODS);
      cfg = new AbstractCacheStoreConfig();
      cs.init(cfg, mockCache(getClass().getName()), null);
   }

   @AfterMethod
   public void tearDown() throws CacheLoaderException {
      cs.stop();
      cs = null;
      cfg = null;
   }

   @Test
   void testSyncExecutorIsSetWhenCfgPurgeSyncIsTrueOnStart() throws Exception {
      cfg.setPurgeSynchronously(true);
      cs.start();
      ExecutorService service = (ExecutorService) ReflectionUtil.getValue(cs, "purgerService");
      assert service instanceof WithinThreadExecutor;
   }

   @Test
   void testAsyncExecutorIsDefaultOnStart() throws Exception {
      cs.start();
      ExecutorService service = (ExecutorService) ReflectionUtil.getValue(cs, "purgerService");
      assert !(service instanceof WithinThreadExecutor);
   }

   public static Cache mockCache(final String name) {
      AdvancedCache cache = mock(AdvancedCache.class);
      ComponentRegistry registry = mock(ComponentRegistry.class);

      when(cache.getName()).thenReturn(name);
      when(cache.getAdvancedCache()).thenReturn(cache);
      when(cache.getComponentRegistry()).thenReturn(registry);
      when(registry.getTimeService()).thenReturn(TIME_SERVICE);
      return cache;
   }
}
