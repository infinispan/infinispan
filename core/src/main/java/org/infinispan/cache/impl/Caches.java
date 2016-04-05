package org.infinispan.cache.impl;

import org.infinispan.Cache;
import org.infinispan.commands.LocalFlagAffectedCommand;
import org.infinispan.context.Flag;

import java.util.Set;

/**
 * Utility methods for dealing with caches.
 *
 * @author Dan Berindei
 * @since 9.0
 */
public class Caches {
   private Caches() {
   }

   public static <K, V> Cache<K, V> getCacheWithFlags(Cache<K, V> cache, LocalFlagAffectedCommand command) {
      Set<Flag> flags = command.getFlags();
      if (flags != null && !flags.isEmpty()) {
         return cache.getAdvancedCache().withFlags(flags.toArray(new Flag[flags.size()]));
      } else {
         return cache;
      }
   }
}
