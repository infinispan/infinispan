/*
 * JBoss, Home of Professional Open Source
 * Copyright 2008, Red Hat Middleware LLC, and individual contributors
 * by the @authors tag. See the copyright.txt in the distribution for a
 * full listing of individual contributors.
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

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.CacheException;
import org.infinispan.atomic.AtomicMap;
import org.infinispan.invocation.Flag;
import org.infinispan.logging.Log;
import org.infinispan.logging.LogFactory;

import java.util.Map;
import java.util.Set;

/**
 * @author Manik Surtani (<a href="mailto:manik AT jboss DOT org">manik AT jboss DOT org</a>)
 * @since 4.0
 */
public class TreeCacheImpl<K, V> extends TreeStructureSupport implements TreeCache<K, V> {
   private static final Log log = LogFactory.getLog(TreeCacheImpl.class);
   private static final boolean trace = log.isTraceEnabled();

   public TreeCacheImpl(Cache<K, V> cache) {
      super(cache, ((AdvancedCache) cache).getBatchContainer(),
            ((AdvancedCache) cache).getInvocationContextContainer());
      assertBatchingSupported(cache.getConfiguration());
      createRoot();
   }

   public Node<K, V> getRoot() {
      return new NodeImpl<K, V>(Fqn.ROOT, cache, batchContainer, icc);
   }

   public Node<K, V> getRoot(Flag... flags) {
      icc.get().setFlags(flags);
      return getRoot();
   }

   public V put(String fqn, K key, V value) {
      return put(Fqn.fromString(fqn), key, value);
   }

   public V put(String fqn, K key, V value, Flag... flags) {
      icc.get().setFlags(flags);
      return put(fqn, key, value);
   }

   public void put(Fqn fqn, Map<? extends K, ? extends V> data) {
      startAtomic();
      try {
         getNode(fqn).putAll(data);
      }
      finally {
         endAtomic();
      }
   }

   public void put(Fqn fqn, Map<? extends K, ? extends V> data, Flag... flags) {
      icc.get().setFlags(flags);
      put(fqn, data);
   }

   public void put(String fqn, Map<? extends K, ? extends V> data) {
      put(Fqn.fromString(fqn), data);
   }

   public void put(String fqn, Map<? extends K, ? extends V> data, Flag... flags) {
      icc.get().setFlags(flags);
      put(fqn, data);
   }

   @SuppressWarnings("unchecked")
   public V remove(Fqn fqn, K key) {
      startAtomic();
      try {
         AtomicMap map = cache.getAtomicMap(new NodeKey(fqn, NodeKey.Type.DATA));
         return map == null ? null : (V) map.remove(key);
      }
      finally {
         endAtomic();
      }
   }

   public V remove(Fqn fqn, K key, Flag... flags) {
      icc.get().setFlags(flags);
      return remove(fqn, key);
   }

   public V remove(String fqn, K key) {
      return remove(Fqn.fromString(fqn), key);
   }

   public V remove(String fqn, K key, Flag... flags) {
      icc.get().setFlags(flags);
      return remove(fqn, key);
   }

   public boolean removeNode(Fqn fqn) {
      if (fqn.isRoot()) return false;
      startAtomic();
      boolean result;
      try {
         if (trace) log.trace("About to remove node " + fqn);
         Node<K, V> n = getNode(fqn.getParent());
         result = n != null && n.removeChild(fqn.getLastElement());
      }
      finally {
         endAtomic();
      }
      if (trace) log.trace("Node successfully removed");
      return result;
   }

   public boolean removeNode(Fqn fqn, Flag... flags) {
      icc.get().setFlags(flags);
      return removeNode(fqn);
   }

   public boolean removeNode(String fqn) {
      return removeNode(Fqn.fromString(fqn));
   }

   public boolean removeNode(String fqn, Flag... flags) {
      icc.get().setFlags(flags);
      return removeNode(fqn);
   }

   public Node<K, V> getNode(Fqn fqn) {
      startAtomic();
      try {
         if (exists(fqn))
            return new NodeImpl<K, V>(fqn, cache, batchContainer, icc);
         else return null;
      }
      finally {
         endAtomic();
      }
   }

   public Node<K, V> getNode(Fqn fqn, Flag... flags) {
      icc.get().setFlags(flags);
      return getNode(fqn);
   }

   public Node<K, V> getNode(String fqn) {
      return getNode(Fqn.fromString(fqn));
   }

   public Node<K, V> getNode(String fqn, Flag... flags) {
      icc.get().setFlags(flags);
      return getNode(fqn);
   }

   @SuppressWarnings("unchecked")
   public V get(Fqn fqn, K key) {
      Map m = cache.getAtomicMap(new NodeKey(fqn, NodeKey.Type.DATA));
      if (m == null) return null;
      return (V) m.get(key);
   }

   public V get(Fqn fqn, K key, Flag... flags) {
      icc.get().setFlags(flags);
      return get(fqn, key);
   }

   public boolean exists(String f) {
      return exists(Fqn.fromString(f));
   }

   public boolean exists(String fqn, Flag... flags) {
      icc.get().setFlags(flags);
      return exists(fqn);
   }

   public boolean exists(Fqn fqn, Flag... flags) {
      icc.get().setFlags(flags);
      return exists(fqn);
   }

   public V get(String fqn, K key) {
      return get(Fqn.fromString(fqn), key);
   }

   public V get(String fqn, K key, Flag... flags) {
      icc.get().setFlags(flags);
      return get(fqn, key);
   }

   public void move(Fqn nodeToMoveFqn, Fqn newParentFqn) throws NodeNotExistsException {
      if (trace) log.trace("Moving node '" + nodeToMoveFqn + "' to '" + newParentFqn + "'");
      if (nodeToMoveFqn == null || newParentFqn == null) throw new NullPointerException("Cannot accept null parameters!");

      if (nodeToMoveFqn.getParent().equals(newParentFqn)) {
         if (trace) log.trace("Not doing anything as this node is equal with its parent");
         // moving onto self!  Do nothing!
         return;
      }

      // Depth first.  Lets start with getting the node we want.
      startAtomic();
      try {
         Node nodeToMove = getNode(nodeToMoveFqn, Flag.FORCE_WRITE_LOCK);
         if (nodeToMove == null) {
            if (trace) log.trace("Did not find the node that needs to be moved. Returning...");
            return; // nothing to do here!
         }
         if (!exists(newParentFqn)) {
            // then we need to silently create the new parent
            createNodeInCache(newParentFqn);
            if (trace) log.trace("The new parent ("+newParentFqn +") did not exists, was created");
         }

         // create an empty node for this new parent
         Fqn newFqn = Fqn.fromRelativeElements(newParentFqn, nodeToMoveFqn.getLastElement());
         createNodeInCache(newFqn);
         Node newNode = getNode(newFqn);
         Map oldData = nodeToMove.getData();
         if (oldData != null && !oldData.isEmpty()) newNode.putAll(oldData);
         for (Object child : nodeToMove.getChildrenNames()) {
            // move kids
            if (trace) log.trace("Moving child " + child);
            Fqn oldChildFqn = Fqn.fromRelativeElements(nodeToMoveFqn, child);
            move(oldChildFqn, newFqn);
         }
         removeNode(nodeToMoveFqn);
      }
      finally {
         endAtomic();
      }
      log.trace("Successfully moved node '" + nodeToMoveFqn + "' to '" + newParentFqn + "'");
   }

   public void move(Fqn nodeToMove, Fqn newParent, Flag... flags) throws NodeNotExistsException {
      icc.get().setFlags(flags);
      move(nodeToMove, newParent);
   }

   public void move(String nodeToMove, String newParent) throws NodeNotExistsException {
      move(Fqn.fromString(nodeToMove), Fqn.fromString(newParent));
   }

   public void move(String nodeToMove, String newParent, Flag... flags) throws NodeNotExistsException {
      icc.get().setFlags(flags);
      move(nodeToMove, newParent);
   }

   public Map<K, V> getData(Fqn fqn) {
      startAtomic();
      try {
         return getNode(fqn).getData();
      }
      finally {
         endAtomic();
      }
   }

   public Map<K, V> getData(Fqn fqn, Flag... flags) {
      icc.get().setFlags(flags);
      return getData(fqn);
   }

   public Set<K> getKeys(String fqn) {
      return getKeys(Fqn.fromString(fqn));
   }

   public Set<K> getKeys(String fqn, Flag... flags) {
      icc.get().setFlags(flags);
      return getKeys(fqn);
   }

   public Set<K> getKeys(Fqn fqn) {
      startAtomic();
      try {
         return getNode(fqn).getKeys();
      }
      finally {
         endAtomic();
      }
   }

   public Set<K> getKeys(Fqn fqn, Flag... flags) {
      icc.get().setFlags(flags);
      return getKeys(fqn);
   }

   public void clearData(String fqn) {
      clearData(Fqn.fromString(fqn));
   }

   public void clearData(String fqn, Flag... flags) {
      icc.get().setFlags(flags);
   }

   public void clearData(Fqn fqn) {
      startAtomic();
      try {
         getNode(fqn).clearData();
      }
      finally {
         endAtomic();
      }
   }

   public void clearData(Fqn fqn, Flag... flags) {
      icc.get().setFlags(flags);
   }

   @SuppressWarnings("unchecked")
   public V put(Fqn fqn, K key, V value) {
      if (trace) log.trace("Start: Putting value under key [" + key + "] for node [" + fqn + "]");
      startAtomic();
      try {
         createNodeInCache(fqn);
         return (V) cache.getAtomicMap(new NodeKey(fqn, NodeKey.Type.DATA)).put(key, value);
      }
      finally {
         endAtomic();
         if (trace) log.trace("End: Putting value under key [" + key + "] for node [" + fqn + "]");
      }
   }

   public V put(Fqn fqn, K key, V value, Flag... flags) {
      icc.get().setFlags(flags);
      return put(fqn, key, value);
   }

   // ------------------ nothing different; just delegate to the cache
   public Cache getCache() {
      return cache;
   }

   public void start() throws CacheException {
      cache.start();
      createRoot();
   }

   public void stop() {
      cache.stop();
   }

   private void createRoot() {
      if (!exists(Fqn.ROOT)) createNodeInCache(Fqn.ROOT);
   }

   public String toString() {
      return cache.toString();
   }
}
