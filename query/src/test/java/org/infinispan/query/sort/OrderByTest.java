package org.infinispan.query.sort;

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

@Test(groups = "functional", testName = "query.parameter.OrderByTest")
public class OrderByTest extends SingleCacheManagerTest {

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
      cache.putAll(Book.data());
   }

   public void useDifferentIndexFieldNamesTests() {
      Query<Book> query = cache.query(String.format("from %s where naming : 'novel' order by title", Book.class.getName()));
      QueryResult<Book> result = query.execute();

      assertThat(result.count().exact()).isTrue();
      assertThat(result.count().value()).isEqualTo(3);
      assertThat(result.list()).extracting("title").containsExactlyInAnyOrder("1984", "The Great Gatsby", "To Kill a Mockingbird");
   }
}
