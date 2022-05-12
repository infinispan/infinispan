package org.infinispan.distribution.group.impl;

import java.util.Map;

import org.infinispan.CacheStream;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.InvocationContext;

/**
 * Control's key grouping.
 *
 * @author Pete Muir
 */
public interface GroupManager {

   /**
    * Get the group for a given key
    *
    * @param key the key for which to get the group
    * @return the group, or null if no group is defined for the key
    */
   Object getGroup(Object key);

   /**
    * Collects all entries belonging to a single group.
    * <p>
    * This method receives a {@link CacheStream} and it must filter the {@link CacheEntry} that belongs to the group.
    * <p>
    * If the cache is transactional, the entries must be stored in the {@link InvocationContext} (with proper read
    * version if applicable).
    *
    * @param stream    The {@link CacheStream} of {@link CacheEntry} to filter.
    * @param ctx       The {@link InvocationContext} to use during its invocation.
    * @param groupName The group name to collect.
    * @param <K>       The key type.
    * @param <V>       The value type.
    * @return A {@link Map} with keys and value.
    */
   <K, V> Map<K, V> collect(CacheStream<? extends CacheEntry<K, V>> stream, InvocationContext ctx, String groupName);
}
