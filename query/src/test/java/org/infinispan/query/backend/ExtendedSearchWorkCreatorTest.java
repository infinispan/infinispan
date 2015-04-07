package org.infinispan.query.backend;

import org.apache.lucene.search.MatchAllDocsQuery;
import org.hibernate.search.annotations.Field;
import org.hibernate.search.annotations.Indexed;
import org.hibernate.search.backend.spi.Work;
import org.hibernate.search.backend.spi.WorkType;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.Search;
import org.infinispan.query.impl.ComponentRegistryUtils;
import org.infinispan.query.impl.DefaultSearchWorkCreator;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import static org.testng.AssertJUnit.assertEquals;

/**
 * Verifies ADD, DELETE and UPDATE behaviour under a custom {@link org.infinispan.query.backend.SearchWorkCreator} that
 * creates extra entities in the index
 */
@Test(groups = "functional", testName = "query.backend.ExtendedSearchWorkCreatorTest")
public class ExtendedSearchWorkCreatorTest extends SingleCacheManagerTest {

   private static class ExtraValuesSearchWorkCreator extends DefaultSearchWorkCreator<Object> implements ExtendedSearchWorkCreator<Object> {

      @Override
      public Collection<Work> createPerEntityWorks(Object value, Serializable id, WorkType workType) {
         // Will generate extra entities based on the items
         List<Work> works = new LinkedList<>(super.createPerEntityWorks(value, id, workType));
         Entity e = (Entity) value;
         for (String item : e.getItems()) {
            Entity newEntity = new Entity(item);
            works.add(new Work(newEntity, "item-" + item, workType));
         }
         return works;
      }

      @Override
      public boolean shouldRemove(SearchWorkCreatorContext context) {
         // Always perform a remove
         return true;
      }
   }

   @Test
   public void testExtendedSearchCreatorForAdd() throws Exception {
      addCustomSearchWorkCreator();

      cache.put(1, new Entity("value", "item-a", "item-b", "item-c"));

      assertEquals(4, numberOfIndexedEntities(Entity.class));
   }

   @Test
   public void testExtendedSearchCreatorForDelete() throws Exception {
      addCustomSearchWorkCreator();

      cache.put(1, new Entity("value", "item-a", "item-b", "item-c"));
      cache.remove(1);

      assertEquals(0, numberOfIndexedEntities(Entity.class));
   }

   @Test
   public void testExtendedSearchCreatorForUpdates() throws Exception {
      addCustomSearchWorkCreator();

      cache.put(1, new Entity("value", "item-a", "item-b", "item-c"));
      assertEquals(4, numberOfIndexedEntities(Entity.class));

      cache.put(1, new Entity("value", "item-a"));
      assertEquals(2, numberOfIndexedEntities(Entity.class));
   }

   private void addCustomSearchWorkCreator() {

      QueryInterceptor queryInterceptor = ComponentRegistryUtils.getQueryInterceptor(cache);
      queryInterceptor.setSearchWorkCreator(new ExtraValuesSearchWorkCreator());
   }

   private int numberOfIndexedEntities(Class<?> clazz) {
      return Search.getSearchManager(cache).getQuery(new MatchAllDocsQuery(), clazz).getResultSize();
   }


   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder cfg = getDefaultStandaloneCacheConfig(false);
      cfg.indexing().index(Index.ALL).addProperty("default.directory_provider", "ram");
      return TestCacheManagerFactory.createCacheManager(cfg);
   }

   @Indexed
   private static class Entity {

      @Field
      private String name;

      public String getName() {
         return name;
      }

      private Set<String> items = new HashSet<>();

      public Set<String> getItems() {
         return items;
      }

      public Entity(String name, String... values) {
         this.name = name;
         Collections.addAll(items, values);
      }

   }

}
