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
   }

   void registerInternalCache(String name, Configuration configuration);

   void registerInternalCache(String name, Configuration configuration, EnumSet<Flag> flags);

   boolean isInternalCache(String name);

   Set<String> getInternalCacheNames();

   void filterInternalCaches(Set<String> names);
}
