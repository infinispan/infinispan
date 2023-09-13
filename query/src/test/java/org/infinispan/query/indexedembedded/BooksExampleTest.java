package org.infinispan.query.indexedembedded;

import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;

import java.util.List;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.test.QueryTestSCI;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.TransactionMode;
import org.testng.annotations.Test;

/**
 * @author Sanne Grinovero &lt;sanne@infinispan.org&gt; (C) 2011 Red Hat Inc.
 */
@Test(groups = "functional", testName = "query.indexedembedded.BooksExampleTest")
public class BooksExampleTest extends SingleCacheManagerTest {

   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder cfg = getDefaultStandaloneCacheConfig(true);
      cfg
            .transaction()
            .transactionMode(TransactionMode.TRANSACTIONAL)
         .indexing()
            .enable()
            .storage(LOCAL_HEAP)
            .addIndexedEntity(Book.class);
      return TestCacheManagerFactory.createCacheManager(QueryTestSCI.INSTANCE, cfg);
   }

   @Test
   public void searchOnEmptyIndex() {
      cache.put("1",
            new Book("Seam in Action",
                  "Dan Allen",
                  "Manning"));
      cache.put("2",
            new Book("Hibernate Search in Action",
                  "Emmanuel Bernard and John Griffin",
                  "Manning"));
      cache.put("3",
            new Book("Megaprogramming Ruby",
                  "Paolo Perrotta",
                  "The Pragmatic Programmers"));

      String query = String.format("FROM %s WHERE title:'in action'", Book.class.getName());
      List<?> list = cache.query(query).list();
      assert list.size() == 2;
   }
}
