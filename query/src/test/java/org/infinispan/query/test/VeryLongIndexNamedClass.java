package org.infinispan.query.test;

import org.hibernate.search.annotations.Field;
import org.hibernate.search.annotations.FieldBridge;
import org.hibernate.search.annotations.Indexed;
import org.hibernate.search.annotations.ProvidedId;
import org.hibernate.search.annotations.Store;
import org.hibernate.search.bridge.builtin.StringBridge;

import java.io.Serializable;

/**
 * Indexed class which index name is very long - taken from the bug description (ISPN-3092).
 *
 * @author Anna Manukyan
 */
@ProvidedId(bridge = @FieldBridge(impl = StringBridge.class))
@Indexed(index = "default_taskworker-java__com.google.appengine.api.datastore.Entity") //Sample long index name taken from the bug description
public class VeryLongIndexNamedClass implements Serializable {
   @Field(store = Store.YES)
   private String name;

   public VeryLongIndexNamedClass() {
   }

   public VeryLongIndexNamedClass(String name) {
      this.name = name;
   }

   public String getName() {
      return name;
   }

   public void setName(String name) {
      this.name = name;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      VeryLongIndexNamedClass that = (VeryLongIndexNamedClass) o;

      if (name != null ? !name.equals(that.name) : that.name != null) return false;

      return true;
   }

   @Override
   public int hashCode() {
      return name != null ? name.hashCode() : 0;
   }

   @Override
   public String toString() {
      return "VeryLongIndexNamedClass{" +
            "name='" + name + '\'' +
            '}';
   }
}
