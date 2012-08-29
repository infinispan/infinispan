package org.infinispan.query.nulls;

import org.apache.lucene.search.Query;
import org.hibernate.search.annotations.Field;
import org.hibernate.search.annotations.Indexed;
import org.hibernate.search.query.dsl.QueryBuilder;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.Search;
import org.infinispan.query.SearchManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import javax.transaction.TransactionManager;
import java.util.List;

/**
 * @author <a href="mailto:mluksa@redhat.com">Marko Luksa</a>
 */
@Test(groups = "functional", testName = "query.nulls.NullCollectionElementsTest")
public class NullCollectionElementsTest extends SingleCacheManagerTest {

   private TransactionManager transactionManager;
   private SearchManager searchManager;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder cfg = getDefaultStandaloneCacheConfig(true);
      cfg
         .indexing()
             .enable()
             .indexLocalOnly(true)
             .addProperty("hibernate.search.default.directory_provider", "ram")
             .addProperty("hibernate.search.lucene_version", "LUCENE_CURRENT");
      EmbeddedCacheManager cacheManager = TestCacheManagerFactory.createCacheManager(cfg);
      Cache<Object, Object> cache = cacheManager.getCache();
      transactionManager = cache.getAdvancedCache().getTransactionManager();
      searchManager = Search.getSearchManager(cache);
      return cacheManager;
   }

   @Test
   public void searchDoesNotReturnNullInCollection() throws Exception {
      transactionManager.begin();
      cache.put("1", new Foo("1"));
      transactionManager.commit();

      transactionManager.begin();
      try {
         cache.remove("1");

         Query query = createQueryBuilder().keyword().onField("bar").matching("1").createQuery();
         List list = searchManager.getQuery(query).list();

         assert list.size() == 0;

      } finally {
         transactionManager.rollback();
      }
   }

   private QueryBuilder createQueryBuilder() {
      return searchManager.buildQueryBuilderForClass(Foo.class).get();
   }


   @Indexed(index = "FooIndex")
   public class Foo {
      private String bar;

      public Foo(String bar) {
         this.bar = bar;
      }

      @Field(name = "bar")
      public String getBar() {
         return bar;
      }
   }

}
