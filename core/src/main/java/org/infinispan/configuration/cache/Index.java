package org.infinispan.configuration.cache;

/**
 * Used to configure indexing of entries in the cache for searching.
 *
 * @author Paul Ferraro
 */
public enum Index {
   NONE, LOCAL, ALL;

   public boolean isEnabled() {
      switch (this) {
         case LOCAL:
         case ALL:
            return true;
         default:
            return false;
      }
   }

   public boolean isLocalOnly() {
      switch (this) {
         case LOCAL:
            return true;
         default:
            return false;
      }
   }

   @Override
   public String toString() {
      // remove conversion to lower case AS7-3835
      return this.name();
   }
}
