package org.infinispan.multimap.impl.function;

import org.infinispan.functional.EntryView;
import org.infinispan.multimap.impl.SetBucket;
import org.infinispan.util.function.SerializableFunction;

/**
 * A base function for the set updates
 *
 * @author Vittorio Rigamonti
 * @since 15.0
 */
public interface SetBucketBaseFunction<K, V, R> extends SerializableFunction<EntryView.ReadWriteEntryView<K, SetBucket<V>>, R> {}
