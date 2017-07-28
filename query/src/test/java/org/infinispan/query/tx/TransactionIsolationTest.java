package org.infinispan.query.tx;

import static org.infinispan.test.Exceptions.assertException;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.fail;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;

import javax.transaction.RollbackException;
import javax.transaction.Transaction;

import org.apache.lucene.search.Query;
import org.hibernate.search.query.dsl.QueryBuilder;
import org.infinispan.Cache;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commons.CacheException;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.distribution.LocalizedCacheTopology;
import org.infinispan.interceptors.DDAsyncInterceptor;
import org.infinispan.query.Search;
import org.infinispan.query.SearchManager;
import org.infinispan.query.test.AnotherGrassEater;
import org.infinispan.query.test.Person;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestException;
import org.infinispan.test.TestingUtil;
import org.infinispan.transaction.LockingMode;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "query.tx.TransactionIsolationTest")
public class TransactionIsolationTest extends MultipleCacheManagersTest {
   private static final Person RADIM = new Person("Radim", "So young!", 29);
   private static final Person TRISTAN = new Person("Tristan", "Too old.", 44);

   @Override
   public Object[] factory() {
      return new Object[] {
            new TransactionIsolationTest().lockingMode(LockingMode.PESSIMISTIC),
            new TransactionIsolationTest().lockingMode(LockingMode.OPTIMISTIC),
      };
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, true);
      builder.transaction().lockingMode(lockingMode);
      builder.indexing().index(Index.ALL)
            .addIndexedEntity(Person.class)
            .addIndexedEntity(AnotherGrassEater.class)
            .addProperty("hibernate.search.default.directory_provider", "local-heap")
            .addProperty("lucene_version", "LUCENE_CURRENT");
      createClusteredCaches(2, builder);
   }

   public void testDuringTransactionPrimary() throws Exception {
      testDuringTransaction(getStringKeyForCache(cache(0)));
   }

   public void testDuringTransactionBackup() throws Exception {
      testDuringTransaction(getStringKeyForCache(cache(1)));
   }

   private void testDuringTransaction(String key) throws Exception {
      cache(0).put(key, RADIM);

      SearchManager sm0 = Search.getSearchManager(cache(0));
      assertEquals(Collections.singletonList(RADIM), getYoungerThan(sm0, 30));

      TestingUtil.withTx(tm(0), () -> {
         cache(0).put(key, TRISTAN);
         // here we could do the check but indexed query does not reflect changes in tx context
         Transaction suspended = tm(0).suspend();

         assertEquals(Collections.singletonList(RADIM), getYoungerThan(sm0, 30));

         tm(0).resume(suspended);
         return null;
      });

      assertEquals(Collections.emptyList(), getYoungerThan(sm0, 30));
      assertEquals(Collections.singletonList(TRISTAN), getYoungerThan(sm0, 100));
   }

   public void testPrepareFailurePrimary() throws Exception {
      testPrepareFailure(getStringKeyForCache(cache(0)));
   }

   public void testPrepareFailureBackup() throws Exception {
      testPrepareFailure(getStringKeyForCache(cache(1)));
   }

   private void testPrepareFailure(String key) throws Exception {
      cache(0).put(key, RADIM);

      SearchManager sm0 = Search.getSearchManager(cache(0));
      assertEquals(Collections.singletonList(RADIM), getYoungerThan(sm0, 30));

      cache(0).getAdvancedCache().getAsyncInterceptorChain().addInterceptor(new FailPrepare(), 0);

      tm(0).begin();
      cache(0).put(key, TRISTAN);
      try {
         tm(0).commit();
         fail("Should rollback");
      } catch (Throwable t) {
         if (t instanceof CacheException) {
            t = t.getCause();
         }
         assertException(RollbackException.class, t);
      }

      // pessimistic mode commits in the prepare command
      Person expected = lockingMode == LockingMode.OPTIMISTIC ? RADIM : TRISTAN;
      assertEquals(expected, cache(0).get(key));
      assertEquals(expected, cache(1).get(key));

      // In pessimistic cache TRISTAN is in cache but it does not match the criteria
      // so the result should be empty
      List<Person> expectedResult = lockingMode == LockingMode.OPTIMISTIC ?
            Collections.singletonList(RADIM) : Collections.emptyList();
      assertEquals(expectedResult, getYoungerThan(sm0, 30));
   }

   @AfterMethod
   public void dropFailPrepare() {
      cache(0).getAdvancedCache().getAsyncInterceptorChain().removeInterceptor(FailPrepare.class);
   }

   private List<Object> getYoungerThan(SearchManager sm, int age) {
      QueryBuilder queryBuilder = sm.buildQueryBuilderForClass(Person.class).get();
      Query query = queryBuilder.range().onField("age").below(age).createQuery();
      return sm.getQuery(query, Person.class).list();
   }

   private String getStringKeyForCache(Cache cache) {
      LocalizedCacheTopology topology = cache.getAdvancedCache().getDistributionManager().getCacheTopology();
      return IntStream.generate(ThreadLocalRandom.current()::nextInt).mapToObj(i -> "key" + i)
            .filter(key -> topology.getDistribution(key).isPrimary()).findAny().get();
   }

   private static class FailPrepare extends DDAsyncInterceptor {
      @Override
      public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
         return invokeNextThenApply(ctx, command, ((rCtx, rCommand, rv) -> {
            throw new TestException("Induced!");
         }));
      }
   }
}
