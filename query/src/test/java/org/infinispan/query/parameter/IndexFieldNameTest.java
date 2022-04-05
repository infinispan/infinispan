package org.infinispan.query.parameter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.Search;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.query.dsl.QueryResult;
import org.infinispan.query.model.Book;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "query.parameter.IndexFieldNameTest")
public class IndexFieldNameTest extends SingleCacheManagerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder builder = getDefaultStandaloneCacheConfig(false);
      builder.indexing().enable()
            .storage(LOCAL_HEAP)
            .addIndexedEntity(Book.class);

      return TestCacheManagerFactory.createCacheManager(builder);
   }

   @BeforeMethod(alwaysRun = true)
   public void beforeMethod() {
      Book book1 = new Book();
      book1.setTitle("is*and");
      book1.setDescription("A pl*ce surrounded by the sea.");
      cache.put(1, book1);

      Book book2 = new Book();
      book2.setTitle("home");
      book2.setDescription("The pl*ce where I'm staying.");
      cache.put(2, book2);
   }

   public void useDifferentIndexFieldNames() {
      QueryFactory factory = Search.getQueryFactory(cache);
      Query<Book> query = factory.create("from org.infinispan.query.model.Book where naming : 'pl*ce' order by label");
      QueryResult<Book> result = query.execute();

      assertThat(result.hitCount()).hasValue(2L);
      assertThat(result.list()).extracting("title").contains("is*and", "home");
   }
}
