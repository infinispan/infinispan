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

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.CacheException;
import org.infinispan.atomic.AtomicMap;
import org.infinispan.config.ConfigurationException;
import org.infinispan.context.Flag;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Map;
import java.util.Set;

/**
 * @author Manik Surtani (<a href="mailto:manik AT jboss DOT org">manik AT jboss DOT org</a>)
 * @since 4.0
 */
public class TreeCacheImpl<K, V> extends TreeStructureSupport implements TreeCache<K, V> {
   private static final Log log = LogFactory.getLog(TreeCacheImpl.class);
   private static final boolean trace = log.isTraceEnabled();


   public TreeCacheImpl(Cache<?, ?> cache) {
      this(cache.getAdvancedCache());
   }

   public TreeCacheImpl(AdvancedCache<?, ?> cache) {
      super(cache, cache.getBatchContainer());
      if (cache.getConfiguration().isIndexingEnabled())
         throw new ConfigurationException("TreeCache cannot be used with a Cache instance configured to use indexing!");
      assertBatchingSupported(cache.getConfiguration());
      createRoot();
   }

   public Node<K, V> getRoot() {
      return new NodeImpl<K, V>(Fqn.ROOT, cache, batchContainer);
   }

   public Node<K, V> getRoot(Flag... flags) {
      tcc.setFlags(flags);
      try {
         return getRoot();
      } finally {
         tcc.suspend();
      }
   }

   public V put(String fqn, K key, V value) {
      return put(Fqn.fromString(fqn), key, value);
   }

   public V put(String fqn, K key, V value, Flag... flags) {
      tcc.setFlags(flags);
      try {
         return put(fqn, key, value);
      } finally {
         tcc.suspend();
      }
   }

   public void put(Fqn fqn, Map<? extends K, ? extends V> data) {
      startAtomic();
      try {
         Node<K, V> n = getNode(fqn);
         if (n == null) createNodeInCache(fqn);
         n = getNode(fqn);
         n.putAll(data);
      } finally {
         endAtomic();
      }
   }

   public void put(Fqn fqn, Map<? extends K, ? extends V> data, Flag... flags) {
      tcc.setFlags(flags);
      try {
         put(fqn, data);
      } finally {
         tcc.suspend();
      }
   }

   public void put(String fqn, Map<? extends K, ? extends V> data) {
      put(Fqn.fromString(fqn), data);
   }

   public void put(String fqn, Map<? extends K, ? extends V> data, Flag... flags) {
      tcc.setFlags(flags);
      try {
         put(fqn, data);
      } finally {
         tcc.suspend();
      }
   }

   public V remove(Fqn fqn, K key) {
      startAtomic();
      try {
         AtomicMap<K, V> map = getAtomicMap(new NodeKey(fqn, NodeKey.Type.DATA));
         return map == null ? null : map.remove(key);
      } finally {
         endAtomic();
      }
   }

   public V remove(Fqn fqn, K key, Flag... flags) {
      tcc.setFlags(flags);
      try {
         return remove(fqn, key);
      } finally {
         tcc.suspend();
      }
   }

   public V remove(String fqn, K key) {
      return remove(Fqn.fromString(fqn), key);
   }

   public V remove(String fqn, K key, Flag... flags) {
      tcc.setFlags(flags);
      try {
         return remove(fqn, key);
      } finally {
         tcc.suspend();
      }
   }

   public boolean removeNode(Fqn fqn) {
      if (fqn.isRoot()) return false;
      startAtomic();
      boolean result;
      try {
         if (trace) log.tracef("About to remove node %s", fqn);
         Node<K, V> n = getNode(fqn.getParent());
         result = n != null && n.removeChild(fqn.getLastElement());
      } finally {
         endAtomic();
      }
      if (trace) log.trace("Node successfully removed");
      return result;
   }

   public boolean removeNode(Fqn fqn, Flag... flags) {
      tcc.setFlags(flags);
      try {
         return removeNode(fqn);
      } finally {
         tcc.suspend();
      }
   }

   public boolean removeNode(String fqn) {
      return removeNode(Fqn.fromString(fqn));
   }

   public boolean removeNode(String fqn, Flag... flags) {
      tcc.setFlags(flags);
      try {
         return removeNode(fqn);
      } finally {
         tcc.suspend();
      }
   }

   public Node<K, V> getNode(Fqn fqn) {
      startAtomic();
      try {
         if (exists(fqn))
            return new NodeImpl<K, V>(fqn, cache, batchContainer);
         else return null;
      } finally {
         endAtomic();
      }
   }

   public Node<K, V> getNode(Fqn fqn, Flag... flags) {
      tcc.setFlags(flags);
      try {
         return getNode(fqn);
      } finally {
         tcc.suspend();
      }
   }

   public Node<K, V> getNode(String fqn) {
      return getNode(Fqn.fromString(fqn));
   }

   public Node<K, V> getNode(String fqn, Flag... flags) {
      tcc.setFlags(flags);
      try {
         return getNode(fqn);
      } finally {
         tcc.suspend();
      }
   }

   public V get(Fqn fqn, K key) {
      Map<K, V> m = getAtomicMap(new NodeKey(fqn, NodeKey.Type.DATA));
      if (m == null) return null;
      return m.get(key);
   }

   public V get(Fqn fqn, K key, Flag... flags) {
      tcc.setFlags(flags);
      try {
         return get(fqn, key);
      } finally {
         tcc.suspend();
      }
   }

   public boolean exists(String f) {
      return exists(Fqn.fromString(f));
   }

   public boolean exists(String fqn, Flag... flags) {
      tcc.setFlags(flags);
      try {
         return exists(fqn);
      } finally {
         tcc.suspend();
      }
   }

   public boolean exists(Fqn fqn, Flag... flags) {
      tcc.setFlags(flags);
      try {
         return exists(fqn);
      } finally {
         tcc.suspend();
      }
   }

   public V get(String fqn, K key) {
      return get(Fqn.fromString(fqn), key);
   }

   public V get(String fqn, K key, Flag... flags) {
      tcc.setFlags(flags);
      try {
         return get(fqn, key);
      } finally {
         tcc.suspend();
      }
   }

   public void move(Fqn nodeToMoveFqn, Fqn newParentFqn) throws NodeNotExistsException {
      if (trace) log.tracef("Moving node '%s' to '%s'", nodeToMoveFqn, newParentFqn);
      if (nodeToMoveFqn == null || newParentFqn == null)
         throw new NullPointerException("Cannot accept null parameters!");

      if (nodeToMoveFqn.getParent().equals(newParentFqn)) {
         if (trace) log.trace("Not doing anything as this node is equal with its parent");
         // moving onto self!  Do nothing!
         return;
      }

      // Depth first.  Lets start with getting the node we want.
      startAtomic();
      try {
         Node<K, V> nodeToMove = getNode(nodeToMoveFqn, Flag.FORCE_WRITE_LOCK);
         if (nodeToMove == null) {
            if (trace) log.trace("Did not find the node that needs to be moved. Returning...");
            return; // nothing to do here!
         }
         if (!exists(newParentFqn)) {
            // then we need to silently create the new parent
            createNodeInCache(newParentFqn);
            if (trace) log.tracef("The new parent (%s) did not exists, was created", newParentFqn);
         }

         // create an empty node for this new parent
         Fqn newFqn = Fqn.fromRelativeElements(newParentFqn, nodeToMoveFqn.getLastElement());
         createNodeInCache(newFqn);
         Node<K, V> newNode = getNode(newFqn);
         Map<K, V> oldData = nodeToMove.getData();
         if (oldData != null && !oldData.isEmpty()) newNode.putAll(oldData);
         for (Object child : nodeToMove.getChildrenNames()) {
            // move kids
            if (trace) log.tracef("Moving child %s", child);
            Fqn oldChildFqn = Fqn.fromRelativeElements(nodeToMoveFqn, child);
            move(oldChildFqn, newFqn);
         }
         removeNode(nodeToMoveFqn);
      } finally {
         endAtomic();
      }
      log.tracef("Successfully moved node '%s' to '%s'", nodeToMoveFqn, newParentFqn);
   }

   public void move(Fqn nodeToMove, Fqn newParent, Flag... flags) throws NodeNotExistsException {
      tcc.setFlags(flags);
      try {
         move(nodeToMove, newParent);
      } finally {
         tcc.suspend();
      }
   }

   public void move(String nodeToMove, String newParent) throws NodeNotExistsException {
      move(Fqn.fromString(nodeToMove), Fqn.fromString(newParent));
   }

   public void move(String nodeToMove, String newParent, Flag... flags) throws NodeNotExistsException {
      tcc.setFlags(flags);
      try {
         move(nodeToMove, newParent);
      } finally {
         tcc.suspend();
      }
   }

   public Map<K, V> getData(Fqn fqn) {
      startAtomic();
      try {
         Node<K, V> node = getNode(fqn);
         if (node == null)
            return null;
         else
            return node.getData();
      } finally {
         endAtomic();
      }
   }

   public Map<K, V> getData(Fqn fqn, Flag... flags) {
      tcc.setFlags(flags);
      try {
         return getData(fqn);
      } finally {
         tcc.suspend();
      }
   }

   public Set<K> getKeys(String fqn) {
      return getKeys(Fqn.fromString(fqn));
   }

   public Set<K> getKeys(String fqn, Flag... flags) {
      tcc.setFlags(flags);
      try {
         return getKeys(fqn);
      } finally {
         tcc.suspend();
      }
   }

   public Set<K> getKeys(Fqn fqn) {
      startAtomic();
      try {
         Node<K, V> node = getNode(fqn);
         if (node == null)
            return null;
         else
            return node.getKeys();
      } finally {
         endAtomic();
      }
   }

   public Set<K> getKeys(Fqn fqn, Flag... flags) {
      tcc.setFlags(flags);
      try {
         return getKeys(fqn);
      } finally {
         tcc.suspend();
      }
   }

   public void clearData(String fqn) {
      clearData(Fqn.fromString(fqn));
   }

   public void clearData(String fqn, Flag... flags) {
      tcc.setFlags(flags);
      try {
         clearData(fqn);
      } finally {
         tcc.suspend();
      }
   }

   public void clearData(Fqn fqn) {
      startAtomic();
      try {
         Node<K, V> node = getNode(fqn);
         if (node != null)
            node.clearData();
      } finally {
         endAtomic();
      }
   }

   public void clearData(Fqn fqn, Flag... flags) {
      tcc.setFlags(flags);
      try {
         clearData(fqn);
      } finally {
         tcc.suspend();
      }
   }

   public V put(Fqn fqn, K key, V value) {
      if (trace) log.tracef("Start: Putting value under key [%s] for node [%s]", key, fqn);
      startAtomic();
      try {
         createNodeInCache(fqn);
         Map<K, V> m = getAtomicMap(new NodeKey(fqn, NodeKey.Type.DATA));
         return m.put(key, value);
      } finally {
         endAtomic();
         if (trace) log.tracef("End: Putting value under key [%s] for node [%s]", key, fqn);
      }
   }

   public V put(Fqn fqn, K key, V value, Flag... flags) {
      tcc.setFlags(flags);
      try {
         return put(fqn, key, value);
      } finally {
         tcc.suspend();
      }
   }

   public Cache<?, ?> getCache() {
      // Retrieve the advanced cache as a way to retrieve
      // the cache behind the cache adapter.
      return cache.getAdvancedCache();
   }

   // ------------------ nothing different; just delegate to the cache
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
