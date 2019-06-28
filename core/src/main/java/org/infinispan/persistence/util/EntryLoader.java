package org.infinispan.persistence.util;

import java.util.concurrent.CompletionStage;

import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.InvocationContext;

public interface EntryLoader<K, V> {
   /**
    * Load and store the entry if present in the data container, returning the entry in the CompletionStage
    * @param ctx context that generated this request
    * @param key key to load from the store
    * @param segment segment of the key to load
    * @param cmd the command that generated this load request
    * @return stage that when complete contains the loaded entry. If the entry is non null the entry is also
    *         written into the underlying data container
    */
   CompletionStage<InternalCacheEntry<K, V>> loadAndStoreInDataContainer(InvocationContext ctx, Object key,
         int segment, FlagAffectedCommand cmd);
}
