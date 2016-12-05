package org.infinispan.counter.api;

/**
 * The storage mode of a counter.
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
public enum Storage {
   /**
    * The counter value is lost when the cluster is restarted/stopped.
    */
   VOLATILE,
   /**
    * The counter value is stored persistently and survives a cluster restart/stop.
    */
   PERSISTENT;

   private static final Storage[] CACHED_VALUES = values();

   public static Storage valueOf(int index) {
      return CACHED_VALUES[index];
   }
}
