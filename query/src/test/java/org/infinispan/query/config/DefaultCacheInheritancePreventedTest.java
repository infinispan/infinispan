package org.infinispan.query.config;

import static org.infinispan.test.TestingUtil.withCacheManager;

import java.io.IOException;

import org.infinispan.Cache;
import org.infinispan.query.Search;
import org.infinispan.query.SearchManager;
import org.infinispan.query.backend.IndexModificationStrategy;
import org.infinispan.query.backend.QueryInterceptor;
import org.infinispan.query.impl.ComponentRegistryUtils;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.CacheManagerCallable;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Similar to QueryParsingTest but that one only looks at the configuration; in this case we check the components are actually
 * started as expected (or not at all, if so expected). See also ISPN-2065.
 *
 * @author Sanne Grinovero
 * @since 5.2
 */
@Test(groups = "unit", testName = "query.config.DefaultCacheInheritancePreventedTest")
public class DefaultCacheInheritancePreventedTest extends AbstractInfinispanTest {

   @Test
   public void verifyIndexDisabledCorrectly() throws IOException {
      withCacheManager(new CacheManagerCallable(
            TestCacheManagerFactory.fromXml("configuration-parsing-test-enbledInDefault.xml")) {
         @Override
         public void call() {
            assertIndexingEnabled(cm.getCache(), true, IndexModificationStrategy.ALL);
            assertIndexingEnabled(cm.getCache("simple"), true, IndexModificationStrategy.ALL);
            assertIndexingEnabled(cm.getCache("not-searchable"), false, null);
            assertIndexingEnabled(cm.getCache("memory-searchable"), true, IndexModificationStrategy.ALL);
            assertIndexingEnabled(cm.getCache("disk-searchable"), true, IndexModificationStrategy.LOCAL_ONLY);
         }
      });
   }

   @Test
   public void verifyIndexEnabledCorrectly() throws IOException {
      withCacheManager(new CacheManagerCallable(
            TestCacheManagerFactory.fromXml("configuration-parsing-test.xml")) {
         @Override
         public void call() {
            assertIndexingEnabled(cm.getCache(), false, null);
            assertIndexingEnabled(cm.getCache("simple"), false, null);
            assertIndexingEnabled(cm.getCache("memory-searchable"), true, IndexModificationStrategy.ALL);
            assertIndexingEnabled(cm.getCache("disk-searchable"), true, IndexModificationStrategy.LOCAL_ONLY);
         }
      });
   }

   /**
    * Verifies that the SearchIntegrator is or is not registered as expected
    * @param expected true if you expect indexing to be enabled
    * @param cache the cache to extract indexing from
    */
   private void assertIndexingEnabled(Cache<Object, Object> cache, boolean expected, IndexModificationStrategy expectedModificationMode) {
      SearchManager searchManager = null;
      try {
         searchManager = Search.getSearchManager(cache);
      }
      catch (IllegalStateException e) {
         // ignored here, we deal with it later
      }
      if (expected && searchManager == null) {
         Assert.fail("SearchManager not found but expected for cache " + cache.getName());
      }
      if (!expected && searchManager != null) {
         Assert.fail("SearchManager not expected but found for cache " + cache.getName());
      }
      //verify as well that the indexing interceptor is (not) there:
      QueryInterceptor queryInterceptor = null;
      try {
         queryInterceptor = ComponentRegistryUtils.getQueryInterceptor(cache);
      }
      catch (IllegalStateException e) {
         // ignored here, we deal with it later
      }
      if (expected && queryInterceptor == null) {
         Assert.fail("QueryInterceptor not found but expected for cache " + cache.getName());
      }
      if (!expected && queryInterceptor != null) {
         Assert.fail("QueryInterceptor not expected but found for cache " + cache.getName());
      }
      if (expected) {
         Assert.assertEquals(queryInterceptor.getIndexModificationMode(), expectedModificationMode);
      }
   }
}
