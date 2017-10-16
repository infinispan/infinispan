package org.infinispan.registry;

import java.util.EnumSet;
import java.util.Set;

import org.infinispan.configuration.cache.Configuration;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;

/**
 * InternalCacheRegistry. Components which create caches for internal use should use this class to
 * create/retrieve them
 *
 * @author Tristan Tarrant
 * @since 7.2
 */
@Scope(Scopes.GLOBAL)
public interface InternalCacheRegistry {
   enum Flag {
      /**
       * means that the cache must be declared only once
       */
      EXCLUSIVE,
      /**
       * means that this cache is visible to users
       */
      USER,
      /**
       * means that his cache requires security to be accessible remotely
       */
      PROTECTED,
      /**
       * means the cache should be made persistent across restarts if global state persistence is enabled
       */
      PERSISTENT,
      /**
       * means that this cache should be queryable
       */
      QUERYABLE,
      /**
       * means that this cache will be global to all nodes when running in clustered mode
       */
      GLOBAL
   }

   /**
    * Registers an internal cache. The cache will be marked as private and volatile
    *
    * @param name
    *           The name of the cache
    * @param configuration
    *           The configuration for the cache
    */
   void registerInternalCache(String name, Configuration configuration);

   /**
    * Registers an internal cache with the specified flags.
    *
    * @param name
    *           The name of the cache
    * @param configuration
    *           The configuration for the cache
    * @param flags
    *           The flags which determine the behaviour of the cache. See {@link Flag}
    */
   void registerInternalCache(String name, Configuration configuration, EnumSet<Flag> flags);

   /**
    * Unregisters  an internal cache
    * @param name The name of the cache
    */
   void unregisterInternalCache(String name);

   /**
    * Returns whether the cache is internal, i.e. it has been registered using the
    * {@link #registerInternalCache(String, Configuration)} method
    */
   boolean isInternalCache(String name);

   /**
    * Returns whether the cache is private, i.e. it has been registered using the
    * {@link #registerInternalCache(String, Configuration, EnumSet<Flag>)} method without the
    * {@link Flag#USER} flag
    */
   boolean isPrivateCache(String name);

   /**
    * Retrieves the names of all the internal caches
    */
   Set<String> getInternalCacheNames();

   /**
    * Removes the private caches from the specified set of cache names
    */
   void filterPrivateCaches(Set<String> names);

   /**
    * Returns whether a particular internal cache has a specific flag
    *
    * @param name the name of the internal cache
    * @param flag the flag to check
    * @return true if the internal cache has the flag, false otherwise
    */
   boolean internalCacheHasFlag(String name, Flag flag);
}
