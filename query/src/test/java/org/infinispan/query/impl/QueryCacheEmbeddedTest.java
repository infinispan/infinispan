package org.infinispan.query.impl;

import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;

import java.util.Set;
import java.util.stream.Collectors;

import org.assertj.core.util.Sets;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.objectfilter.impl.syntax.parser.IckleParsingResult;
import org.infinispan.query.Search;
import org.infinispan.query.core.impl.QueryCache;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.embedded.impl.SearchQueryParsingResult;
import org.infinispan.query.dsl.embedded.testdomain.hsearch.UserHS;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.TransactionMode;
import org.testng.annotations.Test;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
@Test(groups = "functional", testName = "query.impl.QueryCacheEmbeddedTest")
@CleanupAfterMethod
public class QueryCacheEmbeddedTest extends SingleCacheManagerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() {
      ConfigurationBuilder cfg = getDefaultStandaloneCacheConfig(true);
      cfg.transaction()
            .transactionMode(TransactionMode.TRANSACTIONAL)
            .indexing().enable()
            .storage(LOCAL_HEAP)
            .addIndexedEntity(UserHS.class);

      return TestCacheManagerFactory.createCacheManager(cfg);
   }

   public void testQueryCache() {
      // populate our data cache
      UserHS user = new UserHS();
      user.setId(1);
      user.setName("John");
      cache.put("user_" + user.getId(), user);

      // obtain the query cache component
      QueryCache queryCache = ComponentRegistryUtils.getQueryCache(cache);

      // force creation of the lazy internal cache and ensure it is empty
      queryCache.get("someCacheName", "someQueryString", null, "typeDiscriminator", (queryString, acc) -> queryString);

      // ensure internal cache is empty
      queryCache.clear();

      // obtain a reference to the internal query cache via reflection
      Cache<?, ?> internalCache = (Cache<?, ?>) TestingUtil.extractField(QueryCache.class, queryCache, "lazyCache");

      String queryString = "from org.infinispan.query.dsl.embedded.testdomain.hsearch.UserHS u where u.name = 'John'";

      // everything is ready to go

      // ensure the QueryCreator gets invoked
      int[] invoked = {0};
      IckleParsingResult<?> created = queryCache.get(cache.getName(), queryString, null, IckleParsingResult.class, (qs, acc) -> {
         invoked[0]++;
         return null;
      });
      assertEquals(1, invoked[0]);
      assertNull(created);

      // test that the query cache does not have it already
      assertEquals(0, internalCache.size());

      // create and execute a query
      Query<?> query = Search.getQueryFactory(cache).create(queryString);
      query.execute().list();

      // ensure the query cache has it now: one FilterParsingResult and one SearchQueryParsingResult
      assertEquals(2, internalCache.size());
      Set<Class<?>> cacheValueClasses = internalCache.values().stream().map(Object::getClass).collect(Collectors.toSet());
      Set<Class<?>> expectedCachedValueClasses = Sets.newLinkedHashSet(IckleParsingResult.class, SearchQueryParsingResult.class);
      assertEquals(expectedCachedValueClasses, cacheValueClasses);

      // ensure the QueryCreator does not get invoked now
      IckleParsingResult<?> cached = queryCache.get(cache.getName(), queryString, null, IckleParsingResult.class, (qs, acc) -> {
         throw new AssertionError("QueryCreator should not be invoked now");
      });
      assertNotNull(cached);
   }
}
