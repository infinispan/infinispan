package org.infinispan.remoting.transport;

import java.util.Objects;

/**
 * Implementation {@link Address} that contains the site name.
 *
 * @author Pedro Ruivo
 * @since 13.0
 */
public class SiteAddress implements Address {

   private final String name;

   public SiteAddress(String name) {
      this.name = Objects.requireNonNull(name, "Site's name is mandatory");
   }

   @Override
   public int compareTo(@SuppressWarnings("NullableProblems") Address o) {
      if (o instanceof SiteAddress) {
         return name.compareTo(((SiteAddress) o).name);
      }
      return -1;
   }

   @Override
   public int hashCode() {
      return Objects.hash(name);
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) {
         return true;
      }
      if (o == null || getClass() != o.getClass()) {
         return false;
      }
      SiteAddress that = (SiteAddress) o;
      return name.equals(that.name);
   }

   @Override
   public String toString() {
      return "SiteAddress{" +
            "name='" + name + '\'' +
            '}';
   }
}
