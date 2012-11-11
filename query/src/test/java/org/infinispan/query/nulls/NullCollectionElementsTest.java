package org.infinispan.query.nulls;

import org.apache.lucene.search.Query;
import org.hibernate.search.annotations.Field;
import org.hibernate.search.annotations.Indexed;
import org.hibernate.search.query.dsl.QueryBuilder;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.Search;
import org.infinispan.query.SearchManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import java.util.List;
import java.util.concurrent.Callable;

import static org.infinispan.test.TestingUtil.withTx;

/**
 * @author <a href="mailto:mluksa@redhat.com">Marko Luksa</a>
 */
@Test(groups = "functional", testName = "query.nulls.NullCollectionElementsTest")
public class NullCollectionElementsTest extends SingleCacheManagerTest {

   private SearchManager searchManager;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder cfg = getDefaultStandaloneCacheConfig(true);
      cfg
         .indexing()
             .enable()
             .indexLocalOnly(true)
             .addProperty("default.directory_provider", "ram")
             .addProperty("lucene_version", "LUCENE_CURRENT");
      return TestCacheManagerFactory.createCacheManager(cfg);
   }

   @Override
   protected void setup() throws Exception {
      super.setup();
      searchManager = Search.getSearchManager(cache);
   }

   @Test
   public void searchDoesNotReturnNullInCollection() throws Exception {
      withTx(tm(), new Callable<Void>() {
         @Override
         public Void call() throws Exception {
            cache.put("1", new Foo("1"));
            return null;
         }
      });

      withTx(tm(), new Callable<Void>() {
         @Override
         public Void call() throws Exception {
            cache.remove("1");
            Query query = createQueryBuilder().keyword().onField("bar").matching("1").createQuery();
            List list = searchManager.getQuery(query).list();
            assert list.size() == 0;
            return null;
         }
      });
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
