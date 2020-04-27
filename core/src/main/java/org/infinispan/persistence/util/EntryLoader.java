package org.infinispan.persistence.util;

import java.util.concurrent.CompletionStage;

import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.ImmutableContext;

/**
 * Interface that describes methods used for loading entries from the underlying
 * {@link org.infinispan.persistence.manager.PersistenceManager} and store those entries into the
 * {@link org.infinispan.container.DataContainer} if necessary.
 * @param <K> key type
 * @param <V> value type
 * @since 10.0
 * @author wburns
 */
public interface EntryLoader<K, V> {
   /**
    * Load and store the entry if present in the data container, returning the entry in the CompletionStage
    * @param ctx context that generated this request
    * @param key key to load from the store
    * @param segment segment of the key to load
    * @param cmd the command that generated this load request
    * @return stage that when complete contains the loaded entry. If the entry is non null the entry is also
    *         written into the underlying data container
    * @since 10.0
    */
   CompletionStage<InternalCacheEntry<K, V>> loadAndStoreInDataContainer(InvocationContext ctx, Object key,
         int segment, FlagAffectedCommand cmd);

   /**
    * Load and store the entry if present in the data container, returning the entry in the CompletionStage.
    *
    * @param key     key to load from the store
    * @param segment segment of the key to load
    * @return stage that when complete contains the loaded entry. If the entry is non null the entry is also written
    * into the underlying data container
    * @since 10.0
    */
   default CompletionStage<InternalCacheEntry<K, V>> loadAndStoreInDataContainer(K key, int segment) {
      return loadAndStoreInDataContainer(ImmutableContext.INSTANCE, key, segment, null);
   }
}
