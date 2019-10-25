package org.infinispan.commons.api;

import java.util.EnumSet;

import org.infinispan.commons.configuration.BasicConfiguration;

/**
 * Administrative cache container operations.
 *
 * @author Tristan Tarrant
 * @since 9.2
 */

public interface CacheContainerAdmin<C extends CacheContainerAdmin, A extends BasicConfiguration> {

   /**
    * Flags which affect only administrative operations
    *
    * @author Tristan Tarrant
    * @since 9.2
    */
   enum AdminFlag {
      /**
       * If the operation affects configuration, make it permanent, which means it will survive restarts.
       * If the server cannot honor this flag an error will be returned
       * @deprecated configurations are now always permanent by default. Use the {@link #VOLATILE} flag to obtain the opposite behaviour.
       */
      @Deprecated
      PERMANENT,
      VOLATILE;

      private static final AdminFlag[] CACHED_VALUES = AdminFlag.values();

      public static AdminFlag valueOf(int index) {
         return CACHED_VALUES[index];
      }

      public static EnumSet<AdminFlag> fromString(String s) {
         EnumSet<AdminFlag> flags = EnumSet.noneOf(AdminFlag.class);
         if (s != null) {
            for (String name : s.split(",")) {
               flags.add(AdminFlag.valueOf(name));
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
    * @return the cache
    *
    * @throws org.infinispan.commons.CacheException if a cache with the same name already exists
    */
   <K, V> BasicCache<K, V> createCache(String name, String template);

   /**
    * Creates a cache on the container using the specified template.
    *
    * @param name     the name of the cache to create
    * @param configuration the configuration to use for the cache. If null, the configuration marked as default on the container
    *                 will be used
    * @return the cache
    *
    * @throws org.infinispan.commons.CacheException if a cache with the same name already exists
    */
   <K, V> BasicCache<K, V> createCache(String name, A configuration);

   /**
    * Retrieves an existing cache or creates one using the specified template if it doesn't exist
    *
    * @param name     the name of the cache to create
    * @param template the template to use for the cache. If null, the configuration marked as default on the container
    *                 will be used
    * @return the cache
    */
   <K, V> BasicCache<K, V> getOrCreateCache(String name, String template);

   /**
    * Retrieves an existing cache or creates one using the specified template if it doesn't exist
    *
    * @param name     the name of the cache to create
    * @param configuration the configuration to use for the cache. If null, the configuration marked as default on the container
    *                 will be used
    * @return the cache
    */
   <K, V> BasicCache<K, V> getOrCreateCache(String name, A configuration);

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
