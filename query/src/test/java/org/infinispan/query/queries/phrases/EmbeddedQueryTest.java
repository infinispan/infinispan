package org.infinispan.query.queries.phrases;

import static org.junit.Assert.assertEquals;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.CacheQuery;
import org.infinispan.query.Search;
import org.infinispan.query.test.Author;
import org.infinispan.query.test.Book;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "query.queries.phrases.EmbeddedQueryTest")
public class EmbeddedQueryTest extends SingleCacheManagerTest {

   public EmbeddedQueryTest() {
      cleanup = CleanupPhase.AFTER_METHOD;
   }

   private <T> CacheQuery<T> createCacheQuery(Class<T> clazz, String alias, String predicate) {
      String queryStr = String.format("FROM %s %s WHERE %s", clazz.getName(), alias, predicate);
      return Search.getSearchManager(cache).getQuery(queryStr);
   }

   public void testSimpleQuery() {
      assertEquals(0, cache.size());
      cache.put("author#1", new Author("author1", "surname1"));
      cache.put("author#2", new Author("author2", "surname2"));
      cache.put("author#3", new Author("author3", "surname3"));
      assertEquals(3, cache.size());

      CacheQuery<Author> query = createCacheQuery(Author.class, "a", "a.name:'author1'");
      List<Author> result = query.list();
      assertEquals(1, result.size());
      assertEquals("surname1", result.get(0).getSurname());
   }

   public void testEmbeddedQuery() {
      assertEquals(0, cache.size());
      Author a1 = new Author("author1", "surname1");
      Author a2 = new Author("author2", "surname2");
      Author a3 = new Author("author3", "surname3");
      Set<Author> aSet1 = new HashSet<>();
      aSet1.add(a1);
      aSet1.add(a2);
      Set<Author> aSet2 = new HashSet<>();
      aSet2.add(a1);
      aSet2.add(a3);
      Set<Author> aSet3 = new HashSet<>();

      Book book1 = new Book("Book1", "Some very interesting book", aSet1);
      Book book2 = new Book("Book2", "Not so interesting book", aSet2);
      Book book3 = new Book("Book3", "Book of unknown author", aSet3);

      cache.put("book#1", book1);
      cache.put("book#2", book2);
      cache.put("book#3", book3);
      assertEquals(3, cache.size());

      CacheQuery<Book> query = createCacheQuery(Book.class, "b", "b.authors.name:'author1'");
      List<Book> result = query.list();
      assertEquals(2, result.size());

      query = createCacheQuery(Book.class, "b", "b.description:'interesting'");
      result = query.list();
      assertEquals(2, result.size());
   }

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder cfg = getDefaultStandaloneCacheConfig(true);
      cfg
            .indexing().enable()
            .addIndexedEntity(Book.class)
            .addIndexedEntity(Author.class)
            .addProperty("default.directory_provider", "local-heap")
            .addProperty("lucene_version", "LUCENE_CURRENT");
      return TestCacheManagerFactory.createCacheManager(cfg);
   }
}
