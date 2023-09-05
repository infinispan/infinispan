package org.infinispan.query.parameter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;

import org.infinispan.commons.api.query.Query;
import org.infinispan.commons.api.query.QueryResult;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.model.Book;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "query.parameter.FullTextParameterTest")
public class FullTextParameterTest extends SingleCacheManagerTest {

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
      Book book = new Book();
      book.setTitle("island");
      book.setDescription("A place surrounded by the sea.");
      cache.put(1, book);
   }

   public void fulltext() {
      Query<Book> query = cache.query("from org.infinispan.query.model.Book where naming : :description");
      query.setParameter("description", "place");
      QueryResult<Book> result = query.execute();

      assertThat(result.count().value()).isEqualTo(1);
      assertThat(result.list()).extracting("title").contains("island");
   }

   public void generic() {
      Query<Book> query = cache.query("from org.infinispan.query.model.Book where title = :title");
      query.setParameter("title", "island");
      QueryResult<Book> result = query.execute();

      assertThat(result.count().value()).isEqualTo(1);
      assertThat(result.list()).extracting("title").contains("island");
   }
}
