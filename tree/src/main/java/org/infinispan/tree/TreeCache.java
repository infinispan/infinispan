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
package org.infinispan.tree;

import org.infinispan.Cache;
import org.infinispan.CacheException;
import org.infinispan.context.Flag;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.lifecycle.Lifecycle;

import java.util.Map;
import java.util.Set;

/**
 * This is a tree-like facade around a {@link Cache} allowing for efficient tree-style access to cached data.
 * <p/>
 * The primary purpose of this interface is to allow for efficient caching of tree-like structures such as directories,
 * as well as to provide a compatibility layer with JBoss Cache 3.x and earlier.
 * <p/>
 * For most purposes, we expect people to use the {@link Cache} interface directly as it is simpler.
 * <p/>
 * The tree API assumes that a collection of {@link Node}s, organized in a tree structure underneath a root node,
 * contains key/value attributes of data.
 * <p/>
 * Any locking happens on a node-level granularity, which means that all attributes on a node are atomic and in terms of
 * locking, is coarse grained.  At the same time, replication is fine grained, and only modified attributes in a Node
 * are replicated.
 * <p/>
 * Obtaining a TreeCache is done using the {@link TreeCacheFactory}.
 * <pre>
 *   Cache cache = new DefaultCacheFactory().getCache();
 *   TreeCacheFactory tcf = new TreeCacheFactory();
 *   TreeCache tree = tcf.createTreeCache(cache);
 * </pre>
 *
 * @author Manik Surtani (<a href="mailto:manik AT jboss DOT org">manik AT jboss DOT org</a>)
 * @see Node
 * @since 4.0
 */
public interface TreeCache<K, V> extends Lifecycle {
   /**
    * Returns the root node of this cache.
    *
    * @return the root node
    */
   Node<K, V> getRoot();

   Node<K, V> getRoot(Flag... flags);

   /**
    * Associates the specified value with the specified key for a {@link Node} in this cache. If the {@link Node}
    * previously contained a mapping for this key, the old value is replaced by the specified value.
    *
    * @param fqn   <b><i>absolute</i></b> {@link Fqn} to the {@link Node} to be accessed.
    * @param key   key with which the specified value is to be associated.
    * @param value value to be associated with the specified key.
    * @return previous value associated with specified key, or <code>null</code> if there was no mapping for key. A
    *         <code>null</code> return can also indicate that the Node previously associated <code>null</code> with the
    *         specified key, if the implementation supports null values.
    * @throws IllegalStateException if the cache is not in a started state.
    */
   V put(Fqn fqn, K key, V value);

   V put(Fqn fqn, K key, V value, Flag... flags);

   /**
    * Convenience method that takes a string representation of an Fqn.  Otherwise identical to {@link #put(Fqn, Object,
    * Object)}
    *
    * @param fqn   String representation of the Fqn
    * @param key   key with which the specified value is to be associated.
    * @param value value to be associated with the specified key.
    * @return previous value associated with specified key, or <code>null</code> if there was no mapping for key. A
    *         <code>null</code> return can also indicate that the Node previously associated <code>null</code> with the
    *         specified key, if the implementation supports null values.
    * @throws IllegalStateException if the cache is not in a started state
    */

   V put(String fqn, K key, V value);

   V put(String fqn, K key, V value, Flag... flags);

   /**
    * Copies all of the mappings from the specified map to a {@link Node}.
    *
    * @param fqn  <b><i>absolute</i></b> {@link Fqn} to the {@link Node} to copy the data to
    * @param data mappings to copy
    * @throws IllegalStateException if the cache is not in a started state
    */
   void put(Fqn fqn, Map<? extends K, ? extends V> data);

   void put(Fqn fqn, Map<? extends K, ? extends V> data, Flag... flags);

   /**
    * Convenience method that takes a string representation of an Fqn.  Otherwise identical to {@link #put(Fqn,
    * java.util.Map)}
    *
    * @param fqn  String representation of the Fqn
    * @param data data map to insert
    * @throws IllegalStateException if the cache is not in a started state
    */
   void put(String fqn, Map<? extends K, ? extends V> data);

   void put(String fqn, Map<? extends K, ? extends V> data, Flag... flags);

   /**
    * Removes the mapping for this key from a Node. Returns the value to which the Node previously associated the key,
    * or <code>null</code> if the Node contained no mapping for this key.
    *
    * @param fqn <b><i>absolute</i></b> {@link Fqn} to the {@link Node} to be accessed.
    * @param key key whose mapping is to be removed from the Node
    * @return previous value associated with specified Node's key
    * @throws IllegalStateException if the cache is not in a started state
    */
   V remove(Fqn fqn, K key);

   V remove(Fqn fqn, K key, Flag... flags);

   /**
    * Convenience method that takes a string representation of an Fqn.  Otherwise identical to {@link #remove(Fqn,
    * Object)}
    *
    * @param fqn string representation of the Fqn to retrieve
    * @param key key to remove
    * @return old value removed, or null if the fqn does not exist
    * @throws IllegalStateException if the cache is not in a started state
    */
   V remove(String fqn, K key);

   V remove(String fqn, K key, Flag... flags);

   /**
    * Removes a {@link Node} indicated by absolute {@link Fqn}.
    *
    * @param fqn {@link Node} to remove
    * @return true if the node was removed, false if the node was not found
    * @throws IllegalStateException if the cache is not in a started state
    */
   boolean removeNode(Fqn fqn);

   boolean removeNode(Fqn fqn, Flag... flags);

   /**
    * Convenience method that takes a string representation of an Fqn.  Otherwise identical to {@link #removeNode(Fqn)}
    *
    * @param fqn string representation of the Fqn to retrieve
    * @return true if the node was found and removed, false otherwise
    * @throws IllegalStateException if the cache is not in a started state
    */
   boolean removeNode(String fqn);

   boolean removeNode(String fqn, Flag... flags);

   /**
    * A convenience method to retrieve a node directly from the cache.  Equivalent to calling
    * cache.getRoot().getChild(fqn).
    *
    * @param fqn fqn of the node to retrieve
    * @return a Node object, or a null if the node does not exist.
    * @throws IllegalStateException if the cache is not in a started state
    */
   Node<K, V> getNode(Fqn fqn);

   Node<K, V> getNode(Fqn fqn, Flag... flags);

   /**
    * Convenience method that takes a string representation of an Fqn.  Otherwise identical to {@link #getNode(Fqn)}
    *
    * @param fqn string representation of the Fqn to retrieve
    * @return node, or null if the node does not exist
    * @throws IllegalStateException if the cache is not in a started state
    */
   Node<K, V> getNode(String fqn);

   Node<K, V> getNode(String fqn, Flag... flags);


   /**
    * Convenience method that allows for direct access to the data in a {@link Node}.
    *
    * @param fqn <b><i>absolute</i></b> {@link Fqn} to the {@link Node} to be accessed.
    * @param key key under which value is to be retrieved.
    * @return returns data held under specified key in {@link Node} denoted by specified Fqn.
    * @throws IllegalStateException if the cache is not in a started state
    */
   V get(Fqn fqn, K key);

   V get(Fqn fqn, K key, Flag... flags);

   /**
    * Convenience method that takes a string representation of an Fqn.  Otherwise identical to {@link #get(Fqn,
    * Object)}
    *
    * @param fqn string representation of the Fqn to retrieve
    * @param key key to fetch
    * @return value, or null if the fqn does not exist.
    * @throws IllegalStateException if the cache is not in a started state
    */
   V get(String fqn, K key);

   V get(String fqn, K key, Flag... flags);

   /**
    * Moves a part of the cache to a different subtree.
    * <p/>
    * E.g.:
    * <p/>
    * assume a cache structure such as:
    * <p/>
    * <pre>
    *  /a/b/c
    *  /a/b/d
    *  /a/b/e
    * <p/>
    * <p/>
    *  Fqn f1 = Fqn.fromString("/a/b/c");
    *  Fqn f2 = Fqn.fromString("/a/b/d");
    * <p/>
    *  cache.move(f1, f2);
    * </pre>
    * <p/>
    * Will result in:
    * <pre>
    * <p/>
    * /a/b/d/c
    * /a/b/e
    * <p/>
    * </pre>
    * <p/>
    * and now
    * <p/>
    * <pre>
    *  Fqn f3 = Fqn.fromString("/a/b/e");
    *  Fqn f4 = Fqn.fromString("/a");
    *  cache.move(f3, f4);
    * </pre>
    * <p/>
    * will result in:
    * <pre>
    * /a/b/d/c
    * /a/e
    * </pre>
    * No-op if the node to be moved is the root node.
    * <p/>
    * <b>Note</b>: As of 3.0.0 and when using MVCC locking, more specific behaviour is defined as follows: <ul> <li>A
    * no-op if the node is moved unto itself.  E.g., <tt>move(fqn, fqn.getParent())</tt> will not do anything.</li>
    * <li>If a target node does not exist it will be created silently, to be more consistent with other APIs such as
    * <tt>put()</tt> on a nonexistent node.</li> <li>If the source node does not exist this is a no-op, to be more
    * consistent with other APIs such as <tt>get()</tt> on a nonexistent node.</li> </ul>
    *
    * @param nodeToMove the Fqn of the node to move.
    * @param newParent  new location under which to attach the node being moved.
    * @throws NodeNotExistsException may throw one of these if the target node does not exist or if a different thread
    *                                has moved this node elsewhere already.
    * @throws IllegalStateException  if {@link Cache#getStatus()} would not return {@link ComponentStatus#RUNNING}.
    */
   void move(Fqn nodeToMove, Fqn newParent) throws NodeNotExistsException;

   void move(Fqn nodeToMove, Fqn newParent, Flag... flags) throws NodeNotExistsException;

   /**
    * Convenience method that takes in string representations of Fqns.  Otherwise identical to {@link #move(Fqn, Fqn)}
    *
    * @throws IllegalStateException if {@link Cache#getStatus()} would not return {@link ComponentStatus#RUNNING}.
    */
   void move(String nodeToMove, String newParent) throws NodeNotExistsException;

   void move(String nodeToMove, String newParent, Flag... flags) throws NodeNotExistsException;

   /**
    * Retrieves a defensively copied data map of the underlying node.  A convenience method to retrieving a node and
    * getting data from the node directly.
    *
    * @param fqn
    * @return map of data, or an empty map
    * @throws CacheException
    * @throws IllegalStateException if {@link Cache#getStatus()} would not return {@link ComponentStatus#RUNNING}.
    */
   Map<K, V> getData(Fqn fqn);

   Map<K, V> getData(Fqn fqn, Flag... flags);

   /**
    * Convenience method that takes in a String represenation of the Fqn.  Otherwise identical to {@link
    * #getKeys(Fqn)}.
    */
   Set<K> getKeys(String fqn);

   Set<K> getKeys(String fqn, Flag... flags);

   /**
    * Returns a set of attribute keys for the Fqn. Returns null if the node is not found, otherwise a Set. The set is a
    * copy of the actual keys for this node.
    * <p/>
    * A convenience method to retrieving a node and getting keys from the node directly.
    *
    * @param fqn name of the node
    * @throws IllegalStateException if {@link Cache#getStatus()} would not return {@link ComponentStatus#RUNNING}.
    */
   Set<K> getKeys(Fqn fqn);

   Set<K> getKeys(Fqn fqn, Flag... flags);

   /**
    * Convenience method that takes in a String represenation of the Fqn.  Otherwise identical to {@link
    * #clearData(Fqn)}.
    *
    * @throws IllegalStateException if {@link Cache#getStatus()} would not return {@link ComponentStatus#RUNNING}.
    */
   void clearData(String fqn);

   void clearData(String fqn, Flag... flags);

   /**
    * Removes the keys and properties from a named node.
    * <p/>
    * A convenience method to retrieving a node and getting keys from the node directly.
    *
    * @param fqn name of the node
    * @throws IllegalStateException if {@link Cache#getStatus()} would not return {@link ComponentStatus#RUNNING}.
    */
   void clearData(Fqn fqn);

   void clearData(Fqn fqn, Flag... flags);

   /**
    * @return a reference to the underlying cache instance
    */
   Cache<?, ?> getCache();

   /**
    * Tests if an Fqn exists.  Convenience method for {@link #exists(Fqn)}
    *
    * @param fqn string representation of an Fqn
    * @return true if the fqn exists, false otherwise
    */
   boolean exists(String fqn);

   boolean exists(String fqn, Flag... flags);

   /**
    * Tests if an Fqn exists.
    *
    * @param fqn Fqn to test
    * @return true if the fqn exists, false otherwise
    */
   boolean exists(Fqn fqn);

   boolean exists(Fqn fqn, Flag... flags);
}
