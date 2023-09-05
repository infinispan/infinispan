package org.infinispan.query.queries.phrases;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;
import static org.junit.Assert.assertEquals;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.infinispan.commons.api.query.Query;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.impl.ComponentRegistryUtils;
import org.infinispan.query.test.Author;
import org.infinispan.query.test.Book;
import org.infinispan.search.mapper.mapping.SearchMapping;
import org.infinispan.search.mapper.mapping.metamodel.IndexMetamodel;
import org.infinispan.search.mapper.mapping.metamodel.ObjectFieldMetamodel;
import org.infinispan.search.mapper.mapping.metamodel.ValueFieldMetamodel;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "query.queries.phrases.EmbeddedQueryTest")
public class EmbeddedQueryTest extends SingleCacheManagerTest {

   public EmbeddedQueryTest() {
      cleanup = CleanupPhase.AFTER_METHOD;
   }

   private <T> Query<T> createCacheQuery(Class<T> clazz, String alias, String predicate) {
      String queryStr = String.format("FROM %s %s WHERE %s", clazz.getName(), alias, predicate);
      return cache.query(queryStr);
   }

   public void testSimpleQuery() {
      assertEquals(0, cache.size());
      cache.put("author#1", new Author("author1", "surname1"));
      cache.put("author#2", new Author("author2", "surname2"));
      cache.put("author#3", new Author("author3", "surname3"));
      assertEquals(3, cache.size());

      Query<Author> query = createCacheQuery(Author.class, "a", "a.name:'author1'");
      List<Author> result = query.execute().list();
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

      Query<Book> query = createCacheQuery(Book.class, "b", "b.authors.name:'author1'");
      List<Book> result = query.execute().list();
      assertEquals(2, result.size());

      query = createCacheQuery(Book.class, "b", "b.description:'interesting'");
      result = query.execute().list();
      assertEquals(2, result.size());
   }

   public void testEmbeddedMetamodel() {
      SearchMapping searchMapping = ComponentRegistryUtils.getSearchMapping(cache);
      Map<String, IndexMetamodel> metamodel = searchMapping.metamodel();

      Json make = Json.make(metamodel);
      assertThat(make).isNotNull(); // try to parse as JSON

      // 2 root entities
      assertThat(metamodel).hasSize(2);

      IndexMetamodel indexMetamodel = metamodel.get(Book.class.getName());
      assertThat(indexMetamodel.getIndexName()).isEqualTo(Book.class.getName());
      assertThat(indexMetamodel.getValueFields()).hasSize(4);
      assertThat(indexMetamodel.getObjectFields()).hasSize(1);

      ObjectFieldMetamodel embedded = indexMetamodel.getObjectFields().get("authors");
      assertThat(embedded.isMultiValued()).isTrue();
      assertThat(embedded.isMultiValuedInRoot()).isTrue();
      assertThat(embedded.isNested()).isTrue();
      assertThat(embedded.getValueFields()).hasSize(2);

      ValueFieldMetamodel surname = embedded.getValueFields().get("surname");
      assertThat(surname.isSearchable()).isTrue();
      assertThat(surname.isProjectable()).isFalse();
      ValueFieldMetamodel name = embedded.getValueFields().get("name");
      assertThat(name.getType()).isEqualTo(String.class);
      assertThat(name.getAnalyzer()).hasValue("standard");
   }

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder cfg = getDefaultStandaloneCacheConfig(true);
      cfg
            .indexing().enable()
            .storage(LOCAL_HEAP)
            .addIndexedEntity(Book.class)
            .addIndexedEntity(Author.class);
      return TestCacheManagerFactory.createCacheManager(cfg);
   }
}
