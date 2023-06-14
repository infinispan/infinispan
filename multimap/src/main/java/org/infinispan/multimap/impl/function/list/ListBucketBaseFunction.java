package org.infinispan.multimap.impl.function.list;

import org.infinispan.functional.EntryView;
import org.infinispan.multimap.impl.ListBucket;
import org.infinispan.util.function.SerializableFunction;

/**
 * A base function for the list multimap updates
 *
 * @author Katia Aresti
 * @since 15.0
 */
public interface ListBucketBaseFunction<K, V, R> extends SerializableFunction<EntryView.ReadWriteEntryView<K, ListBucket<V>>, R> {}
