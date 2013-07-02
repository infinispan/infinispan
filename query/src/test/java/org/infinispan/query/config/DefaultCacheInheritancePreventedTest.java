package org.infinispan.query.config;

import java.io.IOException;

import org.infinispan.Cache;
import org.infinispan.query.Search;
import org.infinispan.query.SearchManager;
import org.infinispan.query.backend.LocalQueryInterceptor;
import org.infinispan.query.backend.QueryInterceptor;
import org.infinispan.query.impl.ComponentRegistryUtils;
import org.infinispan.test.CacheManagerCallable;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.infinispan.test.TestingUtil.withCacheManager;

/**
 * Similar to QueryParsingTest but that one only looks at the configuration; in this case we check the components are actually
 * started as expected (or not at all, if so expected). See also ISPN-2065.
 * 
 * @author Sanne Grinovero
 * @since 5.2
 */
@Test(groups = "unit", testName = "query.config.DefaultCacheInheritancePreventedTest")
public class DefaultCacheInheritancePreventedTest {

   @Test
   public void verifyIndexDisabledCorrectly() throws IOException {
      withCacheManager(new CacheManagerCallable(
            TestCacheManagerFactory.fromXml("configuration-parsing-test-enbledInDefault.xml")) {
         @Override
         public void call() {
            assertIndexingEnabled(cm.getCache(), true, QueryInterceptor.class);
            assertIndexingEnabled(cm.getCache("simple"), true, QueryInterceptor.class);
            assertIndexingEnabled(cm.getCache("not-searchable"), false, QueryInterceptor.class);
            assertIndexingEnabled(cm.getCache("memory-searchable"), true, QueryInterceptor.class);
            assertIndexingEnabled(cm.getCache("disk-searchable"), true, LocalQueryInterceptor.class);
         }
      });
   }

   @Test
   public void verifyIndexEnabledCorrectly() throws IOException {
      withCacheManager(new CacheManagerCallable(
            TestCacheManagerFactory.fromXml("configuration-parsing-test.xml")) {
         @Override
         public void call() {
            assertIndexingEnabled(cm.getCache(), false, QueryInterceptor.class);
            assertIndexingEnabled(cm.getCache("simple"), false, QueryInterceptor.class);
            assertIndexingEnabled(cm.getCache("memory-searchable"), true, QueryInterceptor.class);
            assertIndexingEnabled(cm.getCache("disk-searchable"), true, LocalQueryInterceptor.class);
         }
      });
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
