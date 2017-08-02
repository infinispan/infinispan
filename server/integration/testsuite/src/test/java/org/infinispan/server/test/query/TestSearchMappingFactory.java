package org.infinispan.server.test.query;

import org.hibernate.search.annotations.Factory;
import org.hibernate.search.annotations.Field;
import org.hibernate.search.annotations.Indexed;
import org.hibernate.search.cfg.SearchMapping;

/**
 * A factory for {@link SearchMapping}s for testing.
 *
 * @author anistor@redhat.com
 */
public class TestSearchMappingFactory {

   public TestSearchMappingFactory() {
   }

   @SuppressWarnings("unused")
   @Factory
   public SearchMapping buildSearchMapping() {
      SearchMapping searchMapping = new SearchMapping();
      searchMapping.entity(MyOtherEntity.class);
      return searchMapping;
   }

   @Indexed
   public static class MyOtherEntity {

      @Field
      public int i;

      public MyOtherEntity(int i) {
         this.i = i;
      }

      @Override
      public String toString() {
         return "MyOtherEntity{i=" + i + '}';
      }
   }
}
