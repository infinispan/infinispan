package org.infinispan.eviction;

import java.util.Map;
import java.util.concurrent.CompletionStage;

import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;

import net.jcip.annotations.ThreadSafe;

/**
 * Central component that deals with eviction of cache entries.
 * <p />
 * This manager only controls notifications of when entries are evicted.
 * <p />
 * @author Manik Surtani
 * @since 4.0
 */
@ThreadSafe
@Scope(Scopes.NAMED_CACHE)
public interface EvictionManager<K, V> {
   /**
    * Handles notifications of evicted entries
    * @param evicted The entries that were just evicted
    * @return stage that when complete the notifications are complete
    */
   default CompletionStage<Void> onEntryEviction(Map<K, Map.Entry<K, V>> evicted) {
      return onEntryEviction(evicted, null);
   }

   /**
    * Handles notifications of evicted entries based on if the command allow them
    * @param evicted The entries that were just evicted
    * @param command The command that generated the eviction if applicable
    * @return stage that when complete the notifications are complete
    */
   CompletionStage<Void> onEntryEviction(Map<K, Map.Entry<K, V>> evicted, FlagAffectedCommand command);
}
