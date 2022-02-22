package org.infinispan.api.common.tasks;

import java.util.function.Function;

import org.infinispan.api.common.CacheEntry;

import io.smallrye.mutiny.Uni;

/**
 * @since 14.0
 **/
public interface EntryConsumerTask<K, V> extends Function<CacheEntry<K, V>, Uni<CacheEntry<K, V>>> {
}
