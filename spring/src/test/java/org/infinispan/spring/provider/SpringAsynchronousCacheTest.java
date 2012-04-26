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
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertSame;
import static org.testng.AssertJUnit.assertTrue;

import org.infinispan.CacheImpl;
import org.infinispan.client.hotrod.impl.async.NotifyingFutureImpl;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.spring.support.embedded.InfinispanNamedEmbeddedCacheFactoryBean;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.util.concurrent.NoOpFuture;
import org.mockito.Mockito;
import org.springframework.cache.Cache;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

/**
 * <p>
 * An integration test for {@link SpringAsynchronousCache}.
 * </p>
 * <p>
 * In addition to the tests performed by {@link SpringCacheCacheTest} this test
 * verifies that the proper asynchronous methods are called on the underlying cache
 * </p>
 * 
 * @author <a href="mailto:olaf DOT bergner AT gmx DOT de">Olaf Bergner</a>
 * @author Marius Bogoevici
 * @author Ryan Gardner
 * 
 */
@Test(testName = "spring.provider.SpringAsynchronousCacheCacheTest", groups = "unit")
public class SpringAsynchronousCacheTest extends SpringCacheCacheTest {

   @Test
   public void testCachePutUsesAsyncMethod() throws Exception {
      final Object key = "enescu";
      final Object value = "value";
      this.cache.put(key, value);
      Mockito.verify(nativeCache).putAsync(key, value);
      Mockito.verifyNoMoreInteractions(nativeCache);
   }

    @Test
    public void testGetsUseASyncMethod() throws Exception {
        final Object key1 = "key1";
        final Object value = "value";
        this.cache.put(key1, value);
        Mockito.when(nativeCache.getAsync(key1)).thenReturn(new NoOpFuture<Object>(value));
        Cache.ValueWrapper wrapper = this.cache.get(key1);

        Mockito.verify(nativeCache, Mockito.atLeastOnce()).containsKey(key1);
        Mockito.verify(nativeCache, Mockito.atLeastOnce()).getAsync(key1);
        assertEquals(value,wrapper.get());
    }

    @Test
    public void testGetsUseASyncMethodReturnsNullWrapperForObjectNotInCache() throws Exception {
        final Object key2 = "key2";

        Cache.ValueWrapper wrapper = this.cache.get(key2);
        assertNull(wrapper);
        Mockito.verify(nativeCache, Mockito.atLeastOnce()).containsKey(key2);
        Mockito.verify(nativeCache, Mockito.never()).getAsync(key2);
    }

   @Test
   public void testCacheClearUsesAsyncMethod() throws Exception {
      this.cache.clear();
      Mockito.verify(nativeCache).clearAsync();
      Mockito.verifyNoMoreInteractions(nativeCache);
   }

   @Test
   public void testEvictUsesAsyncMethod() throws Exception {
      String somekey = "somekey";
      this.cache.evict(somekey);
      Mockito.verify(nativeCache).removeAsync(somekey);
      Mockito.verifyNoMoreInteractions(nativeCache);
   }

   @Override
   @SuppressWarnings("unchecked")
   protected org.infinispan.Cache<Object, Object> createNativeCache() throws Exception {
      return (org.infinispan.Cache<Object, Object>) Mockito.spy(super.createNativeCache());
   }

   @Override
   protected Cache createCache(final org.infinispan.Cache<Object, Object> nativeCache) {
      return new SpringAsynchronousCache(nativeCache);
   }

}
