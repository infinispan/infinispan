package org.infinispan.rest.framework.impl;

import org.infinispan.rest.framework.PathItem;

/**
 * Path item defined by a constant String.
 *
 * @since 10.0
 */
class StringPathItem extends PathItem {

   private final String path;

   StringPathItem(String path) {
      this.path = path;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      StringPathItem that = (StringPathItem) o;

      return path.equals(that.path);
   }

   @Override
   public int hashCode() {
      return path.hashCode();
   }

   @Override
   public String toString() {
      return path;
   }

   @Override
   public String getPath() {
      return path;
   }
}
