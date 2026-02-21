package org.infinispan.test.hibernate.cache.commons.functional.entities;

import java.io.Serializable;

/**
 * This class should be in a package that is different from the test
 * so that the test and entity that uses this class for its primary
 * key does not have access to private field.
 *
 * @author Gail Badner
 */
public class PK implements Serializable {
   private Long id;

   public PK() {
   }

   public PK(Long id) {
      this.id = id;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) {
         return true;
      }
      if (o == null || getClass() != o.getClass()) {
         return false;
      }

      PK pk = (PK) o;

      return !(id != null ? !id.equals(pk.id) : pk.id != null);

   }

   @Override
   public int hashCode() {
      return id != null ? id.hashCode() : 0;
   }
}
