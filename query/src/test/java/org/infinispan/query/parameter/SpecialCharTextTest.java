package org.infinispan.query.parameter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;

import org.infinispan.commons.api.query.Query;
import org.infinispan.commons.api.query.QueryResult;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.protostream.sampledomain.Book;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "query.parameter.SpecialCharTextTest")
public class SpecialCharTextTest extends SingleCacheManagerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() {
      ConfigurationBuilder builder = getDefaultStandaloneCacheConfig(false);
      builder.indexing().enable()
            .storage(LOCAL_HEAP)
            .addIndexedEntity(Book.class);

      return TestCacheManagerFactory.createCacheManager(builder);
   }

   @BeforeMethod(alwaysRun = true)
   public void beforeMethod() {
      Book book = new Book("is*and", "A pl*ce surrounded by the sea.");
      cache.put(1, book);
   }

   public void fulltext() {
      Query<Book> query = cache.query(String.format("from %s where naming : 'pl*ce'", Book.class.getName()));
      QueryResult<Book> result = query.execute();

      assertThat(result.count().value()).isEqualTo(1);
      assertThat(result.list()).extracting("title").contains("is*and");
   }

   public void generic() {
      Query<Book> query = cache.query(String.format("from %s where title = 'is*and'", Book.class.getName()));
      QueryResult<Book> result = query.execute();

      assertThat(result.count().value()).isEqualTo(1);
      assertThat(result.list()).extracting("title").contains("is*and");
   }
}
