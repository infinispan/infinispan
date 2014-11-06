package org.infinispan.query.impl;

import org.hibernate.hql.lucene.LuceneQueryParsingResult;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.Search;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryBuilder;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.query.dsl.embedded.impl.QueryCache;
import org.infinispan.query.dsl.embedded.testdomain.hsearch.UserHS;
import org.infinispan.query.dsl.impl.BaseQueryBuilder;
import org.infinispan.query.dsl.impl.JPAQueryGenerator;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.util.KeyValuePair;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.annotations.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

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
            .addProperty("default.directory_provider", "ram")
            .addProperty("lucene_version", "LUCENE_CURRENT");
      return TestCacheManagerFactory.createCacheManager(cfg);
   }

   public void testQueryCache() throws Exception {
      // persist one User object to ensure the index exists and queries can be validated against it
      UserHS user = new UserHS();
      user.setId(1);
      user.setName("John");
      cache.put("user_" + user.getId(), user);

      // spy on the query cache
      QueryCache queryCache = TestingUtil.extractGlobalComponent(cacheManager, QueryCache.class);
      QueryCache queryCacheSpy = spy(queryCache);
      TestingUtil.replaceComponent(cacheManager, QueryCache.class, queryCacheSpy, true);

      // obtain the query factory and create a query builder
      QueryFactory qf = Search.getQueryFactory(cache);
      QueryBuilder<Query> queryQueryBuilder = qf.from(UserHS.class)
            .having("name").eq("John")
            .toBuilder();

      // compute the same jpa query as it would be generated for the above query
      String jpaQuery = ((BaseQueryBuilder<Query>) queryQueryBuilder).accept(new JPAQueryGenerator());

      // everything set up, test follows ...

      AtomicReference<Object> lastGetResult = captureLastGetResult(queryCacheSpy);

      KeyValuePair<String, Class> queryCacheKey = new KeyValuePair<String, Class>(jpaQuery, LuceneQueryParsingResult.class);

      // ensure that the query cache does not have it already
      LuceneQueryParsingResult cachedParsingResult = queryCache.get(queryCacheKey);
      assertNull(cachedParsingResult);

      // first attempt to build the query (cache is empty)
      queryQueryBuilder.build();

      // ensure the query cache has it now
      cachedParsingResult = queryCache.get(queryCacheKey);
      assertNotNull(cachedParsingResult);

      // check interaction with query cache - expect a cache miss
      InOrder inOrder = inOrder(queryCacheSpy);
      inOrder.verify(queryCacheSpy, calls(1)).get(queryCacheKey);
      ArgumentCaptor<LuceneQueryParsingResult> captor = ArgumentCaptor.forClass(LuceneQueryParsingResult.class);
      inOrder.verify(queryCacheSpy, calls(1)).put(eq(queryCacheKey), captor.capture());
      inOrder.verifyNoMoreInteractions();
      assertNull(lastGetResult.get());
      assertTrue(captor.getValue() == cachedParsingResult);  // == is intentional here!

      // reset interaction and try again
      reset(queryCacheSpy);
      lastGetResult = captureLastGetResult(queryCacheSpy);

      // second attempt to build the query
      queryQueryBuilder.build();

      // check interaction with query cache - expect a cache hit
      inOrder = inOrder(queryCacheSpy);
      inOrder.verify(queryCacheSpy, calls(1)).get(queryCacheKey);
      inOrder.verify(queryCacheSpy, never()).put(any(KeyValuePair.class), any(LuceneQueryParsingResult.class));
      inOrder.verifyNoMoreInteractions();
      assertTrue(lastGetResult.get() == cachedParsingResult);  // == is intentional here!
   }

   public void testQueryCacheConfigOverriding() throws Exception {
      ConfigurationBuilder queryCacheConfigBuilder = getDefaultStandaloneCacheConfig(true);
      queryCacheConfigBuilder
            .clustering().cacheMode(CacheMode.LOCAL)
            .transaction().transactionMode(TransactionMode.NON_TRANSACTIONAL)
            .expiration().maxIdle(777777, TimeUnit.MILLISECONDS)
            .eviction().maxEntries(42)
            .strategy(EvictionStrategy.LIRS);

      cacheManager.defineConfiguration(QueryCache.QUERY_CACHE_NAME, queryCacheConfigBuilder.build());

      QueryCache queryCache = TestingUtil.extractGlobalComponent(cacheManager, QueryCache.class);

      // a dummy call to force init
      queryCache.get(new KeyValuePair<String, Class>("dontcare", Void.class));

      Cache internalQueryCache = TestingUtil.extractField(queryCache, "lazyCache");
      assertEquals(777777, internalQueryCache.getCacheConfiguration().expiration().maxIdle());
      assertEquals(42, internalQueryCache.getCacheConfiguration().eviction().maxEntries());
   }

   private AtomicReference<Object> captureLastGetResult(QueryCache queryCacheSpy) {
      final AtomicReference<Object> lastResult = new AtomicReference<Object>();
      doAnswer(new Answer<Object>() {
         @Override
         public Object answer(InvocationOnMock invocation) throws Throwable {
            Object result = invocation.callRealMethod();
            lastResult.set(result);
            return result;
         }
      }).when(queryCacheSpy).get(any(KeyValuePair.class));
      return lastResult;
   }
}
