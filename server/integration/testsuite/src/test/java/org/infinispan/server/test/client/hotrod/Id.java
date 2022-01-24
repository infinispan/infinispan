package org.infinispan.server.test.client.hotrod;

import java.io.Serializable;

/**
 * @author Pedro Ruivo
 * @since 9.4
 */
public class Id implements Serializable {
   final byte id;

   public Id(int id) {
      this.id = (byte) id;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      Id id1 = (Id) o;

      if (id != id1.id) return false;

      return true;
   }

   @Override
   public int hashCode() {
      return id;
   }
}
