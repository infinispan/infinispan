package org.infinispan.multimap.impl.function.sortedset;

import org.infinispan.functional.EntryView;
import org.infinispan.multimap.impl.SortedSetBucket;
import org.infinispan.util.function.SerializableFunction;

/**
 * A base function for the sorted set multimap updates
 *
 * @author Katia Aresti
 * @since 15.0
 */
public interface SortedSetBucketBaseFunction<K, V, R> extends SerializableFunction<EntryView.ReadWriteEntryView<K, SortedSetBucket<V>>, R> {}
