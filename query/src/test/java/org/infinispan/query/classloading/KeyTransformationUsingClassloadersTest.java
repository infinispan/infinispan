package org.infinispan.query.classloading;

import java.util.List;

import org.apache.lucene.search.Query;
import org.infinispan.AdvancedCache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.Search;
import org.infinispan.query.SearchManager;
import org.infinispan.query.indexedembedded.Book;
import org.infinispan.query.test.CustomKey;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.TransactionMode;
import org.testng.annotations.Test;

/**
 * @author Sanne Grinovero <sanne@infinispan.org> (C) 2011 Red Hat Inc.
 */
@Test(groups = "functional", testName = "query.classloading.KeyTransformationUsingClassloadersTest")
public class KeyTransformationUsingClassloadersTest extends SingleCacheManagerTest {

   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder cfg = getDefaultStandaloneCacheConfig(true);
      cfg
         .transaction()
            .transactionMode(TransactionMode.TRANSACTIONAL)
         .indexing()
            .index(Index.ALL)
            .addProperty("default.directory_provider", "ram")
            .addProperty("lucene_version", "LUCENE_CURRENT");
      return TestCacheManagerFactory.createCacheManager(cfg);
   }

   @Test
   public void searchWithCustomClassLoader() {
      cache.put( new CustomKey(1,2,3),
            new Book("Seam in Action",
                     "Dan Allen",
                     "Manning"));
      cache.put( new CustomKey(4,5,6),
            new Book("Hibernate Search in Action",
                     "Emmanuel Bernard and John Griffin",
                     "Manning"));
      cache.put( "simple-3",
            new Book("Megaprogramming Ruby",
                     "Paolo Perrotta",
                     "The Pragmatic Programmers"));

      CountingClassLoader classLoader = new CountingClassLoader();
      AdvancedCache<Object, Object> applicationCache = cache.getAdvancedCache().with(classLoader);
      SearchManager qf = Search.getSearchManager(applicationCache);

      assert classLoader.countInvocations.get() == 0;

      Query query = qf.buildQueryBuilderForClass(Book.class)
         .get()
            .phrase()
               .onField("title")
               .sentence("in action")
            .createQuery();

      List<Object> list = qf.getQuery(query).list();
      assert list.size() == 2;
      int invocationsCount = classLoader.countInvocations.get();
      assert invocationsCount >= 1 : "Received instead " + invocationsCount + " invocations";
   }

}
