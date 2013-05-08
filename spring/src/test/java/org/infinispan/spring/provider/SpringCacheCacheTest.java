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

import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.spring.support.embedded.InfinispanNamedEmbeddedCacheFactoryBean;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.springframework.cache.Cache;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * <p>
 * An integration test for {@link SpringCache}.
 * </p>
 * <p>
 * <strong>CREDITS</strong> This test is a shameless copy of Costin Leau's
 * <code>org.springframework.cache.vendor.AbstractNativeCacheTest</code>. The additions made to it
 * are minor.
 * </p>
 * 
 * @author <a href="mailto:olaf DOT bergner AT gmx DOT de">Olaf Bergner</a>
 * @author Marius Bogoevici
 * 
 */
@Test(testName = "spring.provider.SpringCacheCacheTest", groups = "unit")
public class SpringCacheCacheTest extends SingleCacheManagerTest {

   protected final static String CACHE_NAME = "testCache";

   private final InfinispanNamedEmbeddedCacheFactoryBean<Object, Object> fb = new InfinispanNamedEmbeddedCacheFactoryBean<Object, Object>();

   private org.infinispan.Cache<Object, Object> nativeCache;

   private Cache cache;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      return TestCacheManagerFactory.createCacheManager();
   }

   @BeforeMethod
   public void setUp() throws Exception {
      this.nativeCache = createNativeCache();
      this.cache = createCache(this.nativeCache);
   }

   @AfterMethod
   public void tearDown() throws Exception {
      this.nativeCache = null;
      this.cache = null;
   }

   @Test
   public void testCacheName() throws Exception {
      assertEquals(CACHE_NAME, this.cache.getName());
   }

   @Test
   public void testNativeCache() throws Exception {
      assertSame(this.nativeCache, this.cache.getNativeCache());
   }

   @Test
   public void testCachePut() throws Exception {
      final Object key = "enescu";
      final Object value = "george";

      assertNull(this.cache.get(key));
      this.cache.put(key, value);
      assertEquals(value, this.cache.get(key).get());
   }

   @Test
   public void testCachePutSupportsNullValue() throws Exception {
      final Object key = "enescu";
      final Object value = null;
      
      assertNull(this.cache.get(key));
      this.cache.put(key, value);
      assertNull(this.cache.get(key).get());
   }

   @Test
   public void testCacheContains() throws Exception {
      final Object key = "enescu";
      final Object value = "george";

      this.cache.put(key, value);

      assertTrue(this.cache.get(key) != null);
   }

   @Test
   public void testCacheContainsSupportsNullValue() throws Exception {
      final Object key = "enescu";
      final Object value = null;
      
      this.cache.put(key, value);
      
      assertTrue(this.cache.get(key) != null);
   }

   @Test
   public void testCacheClear() throws Exception {
      assertNull(this.cache.get("enescu"));
      this.cache.put("enescu", "george");
      assertNull(this.cache.get("vlaicu"));
      this.cache.put("vlaicu", "aurel");
      this.cache.clear();
      assertNull(this.cache.get("vlaicu"));
      assertNull(this.cache.get("enescu"));
   }

   private org.infinispan.Cache<Object, Object> createNativeCache() throws Exception {
      this.fb.setInfinispanEmbeddedCacheManager(cacheManager);
      this.fb.setBeanName(CACHE_NAME);
      this.fb.setCacheName(CACHE_NAME);
      this.fb.afterPropertiesSet();
      return this.fb.getObject();
   }

   private Cache createCache(final org.infinispan.Cache<Object, Object> nativeCache) {
      return new SpringCache(nativeCache);
   }

}
