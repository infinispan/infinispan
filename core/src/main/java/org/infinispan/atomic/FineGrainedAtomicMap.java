package org.infinispan.atomic;

import java.util.Map;

/**
 * FineGrainedAtomicMap is a special type of Map geared for use in Infinispan. In addition to the properties
 * of {@link AtomicMap}, locking and isolation is applied on keys rather than entire map itself.
 *
 * <b><u>Usage</u></b>
 * <p>
 * FineGrainedAtomicMap should be constructed and "registered" with Infinispan using the {@link AtomicMapLookup} helper.  This
 * helper ensures thread safe construction and registration of AtomicMap instances in Infinispan's data container.  E.g.:
 * <br />
 * <code>
 *    FineGrainedAtomicMap&lt;String, Integer&gt; map = AtomicMapLookup.getFineGrainedAtomicMap(cache, "my_atomic_map_key");
 * </code>
 * </p>
 * <p>
 * This interface, for all practical purposes, is just a marker interface that indicates that maps of this type will
 * be locked atomically in the cache and replicated in a fine grained manner, as it does not add any additional methods
 * to {@link java.util.Map}.
 * </p>
 *
 * @author Vladimir Blagojevic
 * @see AtomicMapLookup
 * @see AtomicMap
 * @since 5.1
 */
public interface FineGrainedAtomicMap<K, V> extends Map<K, V> {
}
