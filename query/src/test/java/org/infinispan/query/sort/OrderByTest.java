package org.infinispan.query.sort;

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

@Test(groups = "functional", testName = "query.parameter.OrderByTest")
public class OrderByTest extends SingleCacheManagerTest {

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

      Book book2 = new Book();
      book2.setTitle("home");
      book2.setDescription("The place where I'm staying.");
      cache.put(2, book2);

      Book book3 = new Book();
      book3.setTitle("space");
      book3.setDescription("Space is the place");
      cache.put(3, book3);
   }

   public void useDifferentIndexFieldNamesTests() {
      Query<Book> query = cache.query("from org.infinispan.query.model.Book where naming : 'place' order by label");
      QueryResult<Book> result = query.execute();

      assertThat(result.count().isExact()).isTrue();
      assertThat(result.count().value()).isEqualTo(3);
      assertThat(result.list()).extracting("title").contains("island", "home", "space");
   }
}
