package org.infinispan.query.impl;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;

import java.util.Set;
import java.util.stream.Collectors;

import org.assertj.core.util.Sets;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.objectfilter.impl.syntax.parser.IckleParsingResult;
import org.infinispan.query.Search;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.embedded.impl.LuceneQueryParsingResult;
import org.infinispan.query.dsl.embedded.impl.QueryCache;
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
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder cfg = getDefaultStandaloneCacheConfig(true);
      cfg.transaction()
            .transactionMode(TransactionMode.TRANSACTIONAL)
            .indexing().index(Index.ALL)
            .addIndexedEntity(UserHS.class)
            .addProperty("default.directory_provider", "local-heap")
            .addProperty("lucene_version", "LUCENE_CURRENT");

      return TestCacheManagerFactory.createCacheManager(cfg);
   }

   public void testQueryCache() {
      // populate our data cache
      UserHS user = new UserHS();
      user.setId(1);
      user.setName("John");
      cache.put("user_" + user.getId(), user);

      // obtain the query cache
      QueryCache queryCache = ComponentRegistryUtils.getQueryCache(cache);

      // force creation of the lazy internal cache and ensure it is empty
      queryCache.clear();

      // obtain the internal query cache
      Cache<?, ?> internalCache = (Cache) TestingUtil.extractField(QueryCache.class, queryCache, "lazyCache");

      String queryString = "from org.infinispan.query.dsl.embedded.testdomain.hsearch.UserHS u where u.name = 'John'";

      // everything is ready to go

      // ensure the QueryCreator gets invoked
      int invoked[] = {0};
      IckleParsingResult created = queryCache.get(queryString, null, IckleParsingResult.class, (qs, acc) -> {
         invoked[0]++;
         return null;
      });
      assertEquals(1, invoked[0]);
      assertNull(created);

      // test that the query cache does not have it already
      assertEquals(0, internalCache.size());

      // create and execute a query
      Query query = Search.getQueryFactory(cache).create(queryString);
      query.list();

      // ensure the query cache has it now: one FilterParsingResult and one LuceneQueryParsingResult
      assertEquals(2, internalCache.size());
      Set<Class<?>> cacheValueClasses = internalCache.entrySet().stream().map(e -> e.getValue().getClass()).collect(Collectors.toSet());
      Set<Class<?>> expectedCachedValueClasses = Sets.newLinkedHashSet(IckleParsingResult.class, LuceneQueryParsingResult.class);
      assertEquals(expectedCachedValueClasses, cacheValueClasses);

      // ensure the QueryCreator does not get invoked now
      IckleParsingResult cached = queryCache.get(queryString, null, IckleParsingResult.class, (qs, acc) -> {
         throw new AssertionError("QueryCreator should not be invoked now");
      });
      assertNotNull(cached);
   }
}
