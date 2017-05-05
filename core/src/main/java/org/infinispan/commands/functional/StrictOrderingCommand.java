package org.infinispan.commands.functional;

import org.infinispan.commands.CommandInvocationId;

/**
 * For (functional) commands that require that they are executed on strictly the previous value.
 *
 * We could base the check on {@link org.infinispan.container.versioning.EntryVersion} rather than last
 * {@link CommandInvocationId} but that would require us to enforce versions in non-tx clustered caches, too.
 */
public interface StrictOrderingCommand<K> {
   /**
    * True if the command requires strict ordering. Setting that to false may be useful if we can guarantee
    * that the entry will be accessed purely using commutative functions (e.g. through a flag).
    */
   default boolean isStrictOrdering() {
      return true;
   }
   CommandInvocationId getLastInvocationId(K key);
   void setLastInvocationId(K key, CommandInvocationId id);
}
