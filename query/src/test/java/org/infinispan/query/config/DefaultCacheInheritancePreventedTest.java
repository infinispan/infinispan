/* 
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
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
package org.infinispan.query.config;

import java.io.IOException;

import org.infinispan.Cache;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.query.Search;
import org.infinispan.query.SearchManager;
import org.infinispan.query.backend.LocalQueryInterceptor;
import org.infinispan.query.backend.QueryInterceptor;
import org.infinispan.query.impl.ComponentRegistryUtils;
import org.infinispan.test.TestingUtil;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Similar to QueryParsingTest but that one only looks at the configuration; in this case we check the components are actually
 * started as expected (or not at all, if so expected). See also ISPN-2065.
 * 
 * @author Sanne Grinovero
 * @since 5.2
 */
@Test(groups = "unit", testName = "config.parsing.QueryParsingTest")
public class DefaultCacheInheritancePreventedTest {

   @Test
   public void verifyIndexDisabledCorrectly() throws IOException {
      DefaultCacheManager cacheManager = new DefaultCacheManager("configuration-parsing-test-enbledInDefault.xml");
      try {
         assertIndexingEnabled(cacheManager.getCache(), true, QueryInterceptor.class);
         assertIndexingEnabled(cacheManager.getCache("simple"), true, QueryInterceptor.class);
         assertIndexingEnabled(cacheManager.getCache("not-searchable"), false, QueryInterceptor.class);
         assertIndexingEnabled(cacheManager.getCache("memory-searchable"), true, QueryInterceptor.class);
         assertIndexingEnabled(cacheManager.getCache("disk-searchable"), true, LocalQueryInterceptor.class);
      }
      finally {
         TestingUtil.killCacheManagers(cacheManager);
      }
   }

   @Test
   public void verifyIndexEnabledCorrectly() throws IOException {
      DefaultCacheManager cacheManager = new DefaultCacheManager("configuration-parsing-test.xml");
      try {
         assertIndexingEnabled(cacheManager.getCache(), false, QueryInterceptor.class);
         assertIndexingEnabled(cacheManager.getCache("simple"), false, QueryInterceptor.class);
         assertIndexingEnabled(cacheManager.getCache("memory-searchable"), true, QueryInterceptor.class);
         assertIndexingEnabled(cacheManager.getCache("disk-searchable"), true, LocalQueryInterceptor.class);
      }
      finally {
         TestingUtil.killCacheManagers(cacheManager);
      }
   }

   /**
    * Verifies that the SearchFactoryIntegrator is or is not registered as expected
    * @param expected true if you expect indexing to be enabled
    * @param cache the cache to extract indexing from
    */
   private void assertIndexingEnabled(Cache<Object, Object> cache, boolean expected, Class<? extends QueryInterceptor> expectedQueryInterceptorType) {
      SearchManager searchManager = null;
      try {
         searchManager = Search.getSearchManager(cache);
      }
      catch (IllegalArgumentException e) {
      }
      if (expected && searchManager == null) {
         Assert.fail("SearchManager not found but expected for cache " + cache.getName());
      }
      if (!expected && searchManager != null) {
         Assert.fail("SearchManager not expected but found for cache " + cache.getName());
      }
      //verify as well that the indexing interceptor is (not) there:
      QueryInterceptor component = null;
      try {
         component = ComponentRegistryUtils.getComponent(cache, expectedQueryInterceptorType);
      }
      catch (IllegalArgumentException e) {
      }
      if (expected && component == null) {
         Assert.fail("QueryInterceptor not found but expected for cache " + cache.getName());
      }
      if (!expected && component != null) {
         Assert.fail("QueryInterceptor not expected but found for cache " + cache.getName());
      }
   }

}
