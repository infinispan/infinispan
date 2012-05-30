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

import net.jcip.annotations.ThreadSafe;
import org.infinispan.Cache;
import org.infinispan.context.Flag;

import java.util.Map;
import java.util.Set;

/**
 * A Node is a {@link Fqn named} logical grouping of data in the {@link TreeCache} API of JBoss {@link Cache}. A node
 * should be used to contain data for a single data record, for example information about a particular person or
 * account.
 * <p/>
 * One purpose of grouping cache data into separate nodes is to minimize transaction locking interference, and increase
 * concurrency.  So for example, when multiple threads or possibly distributed caches are accessing different accounts
 * simultaneously.
 * <p/>
 * A node has references to its children, parent (each node except the root - defined by {@link Fqn#ROOT} - has a single
 * parent) and data contained within the node (as key/value pairs).  The data access methods are similar to the
 * collections {@link Map} interface, but some are read-only or return copies of the underlying data.
 * <p/>
 *
 * @author <a href="mailto:manik AT jboss DOT org">Manik Surtani (manik AT jboss DOT org)</a>
 * @see TreeCache
 * @since 4.0
 */
@ThreadSafe
public interface Node<K, V> {
   /**
    * Returns the parent node. If this is the root node, this method returns <code>this</code>.
    *
    * @return the parent node, or self if this is the root node
    */
   Node<K, V> getParent();

   Node<K, V> getParent(Flag... flags);

   /**
    * Returns an immutable set of children nodes.
    *
    * @return an immutable {@link Set} of child nodes.  Empty {@link Set} if there aren't any children.
    */
   Set<Node<K, V>> getChildren();

   Set<Node<K, V>> getChildren(Flag... flags);

   /**
    * Returns an immutable set of children node names.
    *
    * @return an immutable {@link Set} of child node names.  Empty {@link Set} if there aren't any children.
    */
   Set<Object> getChildrenNames();

   Set<Object> getChildrenNames(Flag... flags);

   /**
    * Returns a map containing the data in this {@link Node}.
    *
    * @return a {@link Map} containing the data in this {@link Node}.  If there is no data, an empty {@link Map} is
    *         returned.  The {@link Map} returned is always immutable.
    */
   Map<K, V> getData();

   Map<K, V> getData(Flag... flags);

   /**
    * Returns a {@link Set} containing the data in this {@link Node}.
    *
    * @return a {@link Set} containing the data in this {@link Node}.  If there is no data, an empty {@link Set} is
    *         returned.  The {@link Set} returned is always immutable.
    */
   Set<K> getKeys();

   Set<K> getKeys(Flag... flags);

   /**
    * Returns the {@link Fqn} which represents the location of this {@link Node} in the cache structure.  The {@link
    * Fqn} returned is absolute.
    *
    * @return The {@link Fqn} which represents the location of this {@link Node} in the cache structure.  The {@link
    *         Fqn} returned is absolute.
    */
   Fqn getFqn();

   /**
    * Adds a child node with the given {@link Fqn} under the current node.  Returns the newly created node.
    * <p/>
    * If the child exists returns the child node anyway.  Guaranteed to return a non-null node.
    * <p/>
    * The {@link Fqn} passed in is relative to the current node.  The new child node will have an absolute fqn
    * calculated as follows: <pre>new Fqn(getFqn(), f)</pre>.  See {@link Fqn} for the operation of this constructor.
    *
    * @param f {@link Fqn} of the child node, relative to the current node.
    * @return the newly created node, or the existing node if one already exists.
    */
   Node<K, V> addChild(Fqn f);

   Node<K, V> addChild(Fqn f, Flag... flags);

   /**
    * Removes a child node specified by the given relative {@link Fqn}.
    * <p/>
    * If you wish to remove children based on absolute {@link Fqn}s, use the {@link TreeCache} interface instead.
    *
    * @param f {@link Fqn} of the child node, relative to the current node.
    * @return true if the node was found and removed, false otherwise
    */
   boolean removeChild(Fqn f);

   boolean removeChild(Fqn f, Flag... flags);

   /**
    * Removes a child node specified by the given name.
    *
    * @param childName name of the child node, directly under the current node.
    * @return true if the node was found and removed, false otherwise
    */
   boolean removeChild(Object childName);

   boolean removeChild(Object childName, Flag... flags);

   /**
    * Returns the child node
    *
    * @param f {@link Fqn} of the child node
    * @return null if the child does not exist.
    */
   Node<K, V> getChild(Fqn f);

   Node<K, V> getChild(Fqn f, Flag... flags);

   /**
    * @param name name of the child
    * @return a direct child of the current node.
    */
   Node<K, V> getChild(Object name);

   Node<K, V> getChild(Object name, Flag... flags);


   /**
    * Associates the specified value with the specified key for this node. If this node previously contained a mapping
    * for this key, the old value is replaced by the specified value.
    *
    * @param key   key with which the specified value is to be associated.
    * @param value value to be associated with the specified key.
    * @return Returns the old value contained under this key.  Null if key doesn't exist.
    */
   V put(K key, V value);

   V put(K key, V value, Flag... flags);

   /**
    * If the specified key is not already associated with a value, associate it with the given value, and returns the
    * Object (if any) that occupied the space, or null.
    * <p/>
    * Equivalent to calling
    * <pre>
    *   if (!node.getKeys().contains(key))
    *     return node.put(key, value);
    *   else
    *     return node.get(key);
    * </pre>
    * <p/>
    * except that this is atomic.
    *
    * @param key   key with which the specified value is to be associated.
    * @param value value to be associated with the specified key.
    * @return previous value associated with specified key, or null if there was no mapping for key.
    */
   V putIfAbsent(K key, V value);

   V putIfAbsent(K key, V value, Flag... flags);

   /**
    * Replace entry for key only if currently mapped to some value. Acts as
    * <pre>
    * if ((node.getKeys().contains(key))
    * {
    *     return node.put(key, value);
    * }
    * else
    *     return null;
    * </pre>
    * <p/>
    * except that this is atomic.
    *
    * @param key   key with which the specified value is associated.
    * @param value value to be associated with the specified key.
    * @return previous value associated with specified key, or <tt>null</tt> if there was no mapping for key.
    */
   V replace(K key, V value);

   V replace(K key, V value, Flag... flags);

   /**
    * Replace entry for key only if currently mapped to given value. Acts as
    * <pre>
    * if (node.get(key).equals(oldValue))
    * {
    *     node.put(key, newValue);
    *     return true;
    * }
    * else
    *     return false;
    * </pre>
    * <p/>
    * except that this is atomic.
    *
    * @param key      key with which the specified value is associated.
    * @param oldValue value expected to be associated with the specified key.
    * @param newValue value to be associated with the specified key.
    * @return true if the value was replaced
    */
   boolean replace(K key, V oldValue, V newValue);

   boolean replace(K key, V oldValue, V newValue, Flag... flags);


   /**
    * Copies all of the mappings from the specified map to this node's map. If any data exists, existing keys are
    * overwritten with the keys in the new map. The behavior is equivalent to:
    * <pre>
    * Node node;
    * for (Map.Entry me : map.entrySet())
    *   node.put(me.getKey(), me.getValue());
    * </pre>
    *
    * @param map map to copy from
    */
   void putAll(Map<? extends K, ? extends V> map);

   void putAll(Map<? extends K, ? extends V> map, Flag... flags);

   /**
    * Similar to {@link #putAll(java.util.Map)} except that it removes any entries that exists in the data map first.
    * Note that this happens atomically, under a single lock.  This is the analogous to doing a {@link #clearData()}
    * followed by a {@link #putAll(java.util.Map)} in the same transaction.
    *
    * @param map map to copy from
    */
   void replaceAll(Map<? extends K, ? extends V> map);

   void replaceAll(Map<? extends K, ? extends V> map, Flag... flags);

   /**
    * Returns the value to which this node maps the specified key. Returns <code>null</code> if the node contains no
    * mapping for this key.
    *
    * @param key key of the data to return
    * @return the value to which this node maps the specified key, or <code>null</code> if the map contains no mapping
    *         for this key
    */
   V get(K key);

   V get(K key, Flag... flags);

   /**
    * Removes the mapping for this key from this node if it is present. Returns the value to which the node previously
    * associated the key, or <code>null</code> if the node contained no mapping for this key
    *
    * @param key key whose mapping is to be removed
    * @return previous value associated with specified key, or <code>null</code> if there was no mapping for key
    */
   V remove(K key);

   V remove(K key, Flag... flags);

   /**
    * Removes all mappings from the node's data map.
    */
   void clearData();

   void clearData(Flag... flags);

   /**
    * @return the number of elements (key/value pairs) in the node's data map.
    */
   int dataSize();

   int dataSize(Flag... flags);

   /**
    * Returns true if the child node denoted by the relative {@link Fqn} passed in exists.
    *
    * @param f {@link Fqn} relative to the current node of the child you are testing the existence of.
    * @return true if the child node denoted by the relative {@link Fqn} passed in exists.
    */
   boolean hasChild(Fqn f);

   boolean hasChild(Fqn f, Flag... flags);

   /**
    * Returns true if the child node denoted by the Object name passed in exists.
    *
    * @param o name of the child, relative to the current node
    * @return true if the child node denoted by the name passed in exists.
    */
   boolean hasChild(Object o);

   boolean hasChild(Object o, Flag... flags);

   /**
    * Tests if a node reference is still valid.  A node reference may become invalid if it has been removed, invalidated
    * or moved, either locally or remotely.  If a node is invalid, it should be fetched again from the cache or a valid
    * parent node.  Operations on invalid nodes will throw a {@link org.infinispan.tree.NodeNotValidException}.
    *
    * @return true if the node is valid.
    */
   boolean isValid();

   void removeChildren();

   void removeChildren(Flag... flags);
}
