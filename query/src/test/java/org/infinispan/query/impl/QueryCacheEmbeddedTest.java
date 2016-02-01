package org.infinispan.query.impl;

import org.hibernate.hql.lucene.LuceneQueryParsingResult;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.Search;
import org.infinispan.query.dsl.QueryBuilder;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.query.dsl.embedded.impl.QueryCache;
import org.infinispan.query.dsl.embedded.impl.QueryEngine;
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

import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.calls;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;

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
            .addProperty("default.directory_provider", "ram")
            .addProperty("lucene_version", "LUCENE_CURRENT");
      return TestCacheManagerFactory.createCacheManager(cfg);
   }

   public void testQueryCache() throws Exception {
      UserHS user = new UserHS();
      user.setId(1);
      user.setName("John");
      cache.put("user_" + user.getId(), user);

      // spy on the query cache
      QueryEngine queryEngine = TestingUtil.extractComponent(cache, QueryEngine.class);
      QueryCache queryCache = (QueryCache) TestingUtil.extractField(QueryEngine.class, queryEngine, "queryCache");
      QueryCache queryCacheSpy = spy(queryCache);
      TestingUtil.replaceField(queryCacheSpy, "queryCache", queryEngine, QueryEngine.class);

      // obtain the query factory and create a query builder
      QueryFactory qf = Search.getQueryFactory(cache);
      QueryBuilder queryQueryBuilder = qf.from(UserHS.class)
            .having("name").eq("John")
            .toBuilder();

      // compute the same jpa query as it would be generated for the above query
      String jpaQuery = ((BaseQueryBuilder) queryQueryBuilder).accept(new JPAQueryGenerator());

      // everything set up, test follows ...

      AtomicReference<Object> lastGetResult = captureLastGetResult(queryCacheSpy);

      KeyValuePair<String, Class> queryCacheKey = new KeyValuePair<String, Class>(jpaQuery, LuceneQueryParsingResult.class);

      // ensure that the query cache does not have it already
      LuceneQueryParsingResult cachedParsingResult = queryCache.get(queryCacheKey);
      assertNull(cachedParsingResult);

      // first attempt to build and execute the query (query cache is empty)
      queryQueryBuilder.build().list();

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

      // second attempt to build and execute the query
      queryQueryBuilder.build().list();

      // check interaction with query cache - expect a cache hit
      inOrder = inOrder(queryCacheSpy);
      inOrder.verify(queryCacheSpy, calls(1)).get(queryCacheKey);
      inOrder.verify(queryCacheSpy, never()).put(any(KeyValuePair.class), any(LuceneQueryParsingResult.class));
      inOrder.verifyNoMoreInteractions();
      assertTrue(lastGetResult.get() == cachedParsingResult);  // == is intentional here!
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
