package org.infinispan.cache.impl;

import org.infinispan.Cache;
import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commons.util.EnumUtil;
import org.infinispan.context.Flag;

/**
 * Utility methods for dealing with caches.
 *
 * @author Dan Berindei
 * @since 9.0
 */
public class Caches {
   private Caches() {
   }

   public static <K, V> Cache<K, V> getCacheWithFlags(Cache<K, V> cache, FlagAffectedCommand command) {
      long flags = command.getFlagsBitSet();
      if (flags != EnumUtil.EMPTY_BIT_SET) {
         return cache.getAdvancedCache().withFlags(EnumUtil.enumArrayOf(flags, Flag.class));
      } else {
         return cache;
      }
   }
}
