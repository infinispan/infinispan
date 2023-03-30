package org.infinispan.configuration.cache;

import java.util.EnumSet;

import org.infinispan.commons.logging.Log;

/**
 * Affects how cache operations will be propagated to the indexes.
 * By default, {@link #AUTO}.
 *
 * @author Fabio Massimo Ercoli &lt;fabiomassimo.ercoli@gmail.com&gt;
 * @since 15.0
 */
public enum IndexingMode {

   /**
    * All the changes to the cache will be immediately applied to the indexes.
    */
   AUTO,

   /**
    * Indexes will be only updated when a reindex is explicitly invoked.
    */
   MANUAL;

   public static IndexingMode requireValid(String value) {
      try {
         return IndexingMode.valueOf(value.toUpperCase());
      } catch (IllegalArgumentException e) {
         throw Log.CONFIG.illegalEnumValue(value, EnumSet.allOf(IndexingMode.class));
      }
   }
}
