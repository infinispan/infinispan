package org.infinispan.multimap.impl.function.multimap;

import org.infinispan.functional.EntryView;
import org.infinispan.multimap.impl.Bucket;
import org.infinispan.util.function.SerializableFunction;

/**
 * A base function for the multimap updates
 *
 * @author Katia Aresti, karesti@redhat.com
 * @since 9.2
 */
public interface BaseFunction<K, V, R> extends SerializableFunction<EntryView.ReadWriteEntryView<K, Bucket<V>>, R> {}
