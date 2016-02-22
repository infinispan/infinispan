package org.infinispan.server.test.query;

import org.hibernate.search.annotations.Field;
import org.hibernate.search.annotations.Indexed;
import org.hibernate.search.annotations.Store;

import java.io.Serializable;

/**
 * @author anistor@redhat.com
 * @since 8.2
 */
@Indexed
public class TestEntity implements Serializable {

   @Field(store = Store.YES)
   private final String name;

   public TestEntity(String name) {
      this.name = name;
   }

   public String getName() {
      return name;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o)
         return true;
      if (o == null || getClass() != o.getClass())
         return false;
      TestEntity person = (TestEntity) o;
      return name.equals(person.name);
   }

   @Override
   public int hashCode() {
      return name.hashCode();
   }
}
