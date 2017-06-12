package org.infinispan.multimap.impl.function;

import java.util.Collection;

import org.infinispan.functional.EntryView;
import org.infinispan.util.function.SerializableFunction;

/**
 * A base function for the multimap updates
 *
 * @author Katia Aresti, karesti@redhat.com
 * @since 9.2
 */
public interface BaseFunction<K, V, R> extends SerializableFunction<EntryView.ReadWriteEntryView<K, Collection<V>>, R> {}
