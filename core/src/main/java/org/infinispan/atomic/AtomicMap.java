package org.infinispan.atomic;

import java.util.Map;

import org.infinispan.util.concurrent.IsolationLevel;

/**
 * This is a special type of Map geared for use in Infinispan.  AtomicMaps have two major characteristics:
 *
 * <ol>
 *    <li>Atomic locking and isolation over the entire collection</li>
 *    <li>Replication of updates through deltas</li>
 * </ol>
 *
 * <b><u>1.  Atomic locking and isolation over the entire collection</u></b>
 * <p>
 * This allows the entire AtomicMap to be locked when making changes even to certain entries within the map, and also
 * isolates the map for safe reading (see {@link IsolationLevel} while concurrent writes may be going on.
 * </p>
 * <br />
 * <b><u>2.  Replication of updates through deltas</u></b>
 * <p>
 * As a performance optimization, when the map is updated the maps do not replicate all entries to other nodes but only
 * the modifications.
 * </p>
 * <br />
 * <b><u>Usage</u></b>
 * <p>
 * AtomicMaps should be constructed and "registered" with Infinispan using the {@link AtomicMapLookup} helper.  This
 * helper ensures threadsafe construction and registration of AtomicMap instances in Infinispan's data container.  E.g.:
 * <br />
 * <code>
 *    AtomicMap&lt;String, Integer&gt; map = AtomicMapLookup.getAtomicMap(cache, "my_atomic_map_key");
 * </code>
 * </p>
 * <p><b><u>Referential Integrity</u></b><br />
 * It is important to note that concurrent readers of an AtomicMap will essentially have the same view of the contents
 * of the underlying structure, but since AtomicMaps use internal proxies, readers are isolated from concurrent writes
 * and {@link IsolationLevel#READ_COMMITTED} and {@link IsolationLevel#REPEATABLE_READ} semantics are guaranteed.
 * However, this guarantee is only present if the values stored in an AtomicMap are <i>immutable</i> (e.g., Strings,
 * primitives, and other immutable types).</p>
 *
 * <p>Mutable value objects which happen to be stored in an AtomicMap may be updated and, prior to being committed,
 * or even replaced in the map, be visible to concurrent readers.  Hence, AtomicMaps are <b><i>not suitable</i></b> for
 * use with mutable value objects.</p>
 * </p>
 * <br />
 * <p>
 * This interface, for all practical purposes, is just a marker interface that indicates that maps of this type will
 * be locked atomically in the cache and replicated in a fine grained manner, as it does not add any additional methods
 * to {@link java.util.Map}.
 * </p>
 *
 * @author Manik Surtani
 * @see AtomicMapLookup
 * @see FineGrainedAtomicMap
 * @since 4.0
 */
public interface AtomicMap<K, V> extends Map<K, V> {
}
