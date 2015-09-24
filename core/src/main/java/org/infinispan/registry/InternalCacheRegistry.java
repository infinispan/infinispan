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
      EXCLUSIVE, // means that the cache must be declared only once
      USER, // means that this cache is visible to users
      PERSISTENT, // means the cache should be made persistent across restarts if global state persistence is enabled
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
    * Returns whether the cache is internal, i.e. it has been registered using the
    * {@link #registerInternalCache(String, Configuration)} method
    */
   boolean isInternalCache(String name);

   /**
    * Retrieves the names of all the internal caches
    */
   Set<String> getInternalCacheNames();

   /**
    * Removes the private caches from the specified set of cache names
    */
   void filterPrivateCaches(Set<String> names);
}
