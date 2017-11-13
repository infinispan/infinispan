package org.infinispan.commons.api;

import java.util.EnumSet;

/**
 * Administrative cache container operations.
 *
 * @author Tristan Tarrant
 * @since 9.2
 */

public interface CacheContainerAdmin<C extends CacheContainerAdmin> {

   /**
    * Flags which affect only administrative operations
    *
    * @author Tristan Tarrant
    * @since 9.2
    */
   enum AdminFlag {
      /**
       * If the operation affects configuration, make it persistent. If the server cannot honor this flag an error will
       * be returned
       */
      PERSISTENT;

      private static final AdminFlag[] CACHED_VALUES = AdminFlag.values();

      public static AdminFlag valueOf(int index) {
         return CACHED_VALUES[index];
      }

      public static EnumSet<AdminFlag> fromString(String s) {
         EnumSet<AdminFlag> flags = EnumSet.noneOf(AdminFlag.class);
         if (s != null) {
            for (String name : s.split(",")) {
               flags.add(AdminFlag.valueOf(name.toUpperCase()));
            }
         }
         return flags;

      }
   }



   /**
    * Creates a cache on the container using the specified template.
    *
    * @param name     the name of the cache to create
    * @param template the template to use for the cache. If null, the configuration marked as default on the container
    *                 will be used
    */
   void createCache(String name, String template);

   /**
    * Removes a cache from the cache container. Any persisted data will be cleared.
    *
    * @param name the name of the cache to remove
    */
   void removeCache(String name);

   /**
    * Sets any additional {@link AdminFlag}s to be used when performing administrative operations.
    *
    * <b>Note:</b> whether an operation supports a certain flag or not is dependent on the configuration and environment.
    * If a flag cannot be honored, the operation will fail with an exception.
    *
    * @param flags
    * @return
    */
   C withFlags(AdminFlag... flags);

   /**
    * Sets any additional {@link AdminFlag}s to be used when performing administrative operations.
    *
    * <b>Note:</b> whether an operation supports a certain flag or not is dependent on the configuration and environment.
    * If a flag cannot be honored, the operation will fail with an exception.
    *
    * @param flags
    * @return
    */
   C withFlags(EnumSet<AdminFlag> flags);
}
