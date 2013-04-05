/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.atomic;

import java.util.Map;
import org.infinispan.util.concurrent.IsolationLevel;

/**
 * This is a special type of Map geared for use in Infinispan.  AtomicMaps have two major characteristics:
 *
 * <ol>
 *    <li>Atomic locking and isolation over the entire collection</li>
 *    <li>Fine-grained serialization of deltas</li>
 * </ol>
 *
 * <b><u>1.  Atomic locking and isolation over the entire collection</u></b>
 * <p>
 * This allows the entire AtomicMap to be locked when making changes even to certain entries within the map, and also
 * isolates the map for safe reading (see {@link IsolationLevel} while concurrent writes may be going on.
 * </p>
 * <br />
 * <b><u>2.  Fine-grained serialization of deltas</u></b>
 * <p>
 * AtomicMap implementations also implement the {@link DeltaAware} interface.  This powerful interface allows the
 * generation and application of deltas, and requires that implementations are capable of tracking changes made to it
 * during the course of a transaction.  This helps since when performing replications to update remote nodes, the
 * <i>entire</i> map need not be serialized and transported all the time, as serializing and transporting {@link Delta}
 * instances would work just as well, and typically be much smaller and hence faster to serialize and transport.
 * </p>
 * <br />
 * <p>
 * Applications requiring either or both of the characteristics described above are encouraged to use AtomicMaps with
 * Infinispan, however the real benefit is often only seen where a combination of both characteristics are required.
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
 * @see DeltaAware
 * @see Delta
 * @see AtomicHashMap
 * @see AtomicMapLookup
 * @since 4.0
 */
public interface AtomicMap<K, V> extends Map<K, V> {
}
