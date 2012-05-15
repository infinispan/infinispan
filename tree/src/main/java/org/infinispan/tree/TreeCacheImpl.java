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
import org.infinispan.DecoratedCache;
import org.infinispan.atomic.AtomicMap;
import org.infinispan.config.ConfigurationException;
import org.infinispan.context.Flag;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

/**
 * @author Manik Surtani (<a href="mailto:manik AT jboss DOT org">manik AT jboss DOT org</a>)
 * @since 4.0
 */
public class TreeCacheImpl<K, V> extends TreeStructureSupport implements TreeCache<K, V> {
   private static final Log log = LogFactory.getLog(TreeCacheImpl.class);
   private static final boolean trace = log.isTraceEnabled();

   private final EnumSet<Flag> flagsOnCache;


   public TreeCacheImpl(Cache<?, ?> cache) {
      this(cache.getAdvancedCache());
   }

   public TreeCacheImpl(AdvancedCache<?, ?> cache) {
      super(cache, cache.getBatchContainer());
      if (cache.getConfiguration().isIndexingEnabled())
         throw new ConfigurationException("TreeCache cannot be used with a Cache instance configured to use indexing!");
      assertBatchingSupported(cache.getConfiguration());
      if (cache instanceof DecoratedCacheAdapter) {
         flagsOnCache = ((DecoratedCacheAdapter) cache).getFlags();
      } else if (cache instanceof DecoratedCache) {
         flagsOnCache = ((DecoratedCache) cache).getFlags();
      } else {
         flagsOnCache = null;
      }
      createRoot();
   }

   private void setFlagsOnContext(Flag... flags) {
      if (flags != null && flags.length > 0) {
         if (flagsOnCache == null)
            tcc.setFlags(flags);
         else {
            EnumSet<Flag> tmp = flagsOnCache.clone();
            tmp.addAll(Arrays.asList(flags));
            tcc.setFlags(tmp);
         }
      } else if (flagsOnCache != null) {
         tcc.setFlags(flagsOnCache);
      }
   }

   @Override
   public Node<K, V> getRoot() {
      return getRoot(null);
   }

   @Override
   public Node<K, V> getRoot(Flag... flags) {
      setFlagsOnContext(flags);
      try {
         return new NodeImpl<K, V>(Fqn.ROOT, cache, batchContainer);
      } finally {
         tcc.suspend();
      }
   }

   @Override
   public V put(String fqn, K key, V value) {
      return put(Fqn.fromString(fqn), key, value, null);
   }

   @Override
   public V put(String fqn, K key, V value, Flag... flags) {
      return put(Fqn.fromString(fqn), key, value, flags);
   }

   @Override
   public void put(Fqn fqn, Map<? extends K, ? extends V> data) {
      put(fqn, data, null);
   }

   @Override
   public void put(Fqn fqn, Map<? extends K, ? extends V> data, Flag... flags) {
      setFlagsOnContext(flags);
      try {
         startAtomic();
         try {
            Node<K, V> n = getNode(fqn);
            if (n == null) createNodeInCache(fqn);
            n = getNode(fqn);
            n.putAll(data);
         } finally {
            endAtomic();
         }
      } finally {
         tcc.suspend();
      }
   }

   @Override
   public void put(String fqn, Map<? extends K, ? extends V> data) {
      put(Fqn.fromString(fqn), data, null);
   }

   @Override
   public void put(String fqn, Map<? extends K, ? extends V> data, Flag... flags) {
      put(Fqn.fromString(fqn), data, flags);
   }

   @Override
   public V remove(Fqn fqn, K key) {
      return remove(fqn, key, null);
   }

   @Override
   public V remove(Fqn fqn, K key, Flag... flags) {
      setFlagsOnContext(flags);
      try {
         startAtomic();
         try {
            AtomicMap<K, V> map = getAtomicMap(new NodeKey(fqn, NodeKey.Type.DATA));
            return map == null ? null : map.remove(key);
         } finally {
            endAtomic();
         }
      } finally {
         tcc.suspend();
      }
   }

   @Override
   public V remove(String fqn, K key) {
      return remove(Fqn.fromString(fqn), key, null);
   }

   @Override
   public V remove(String fqn, K key, Flag... flags) {
      return remove(Fqn.fromString(fqn), key, flags);
   }

   @Override
   public boolean removeNode(Fqn fqn) {
      return removeNode(fqn, null);
   }

   @Override
   public boolean removeNode(Fqn fqn, Flag... flags) {
      setFlagsOnContext(flags);
      try {
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
      } finally {
         tcc.suspend();
      }
   }

   @Override
   public boolean removeNode(String fqn) {
      return removeNode(Fqn.fromString(fqn), null);
   }

   @Override
   public boolean removeNode(String fqn, Flag... flags) {
      return removeNode(Fqn.fromString(fqn), flags);
   }

   @Override
   public Node<K, V> getNode(Fqn fqn) {
      return getNode(fqn, null);
   }

   @Override
   public Node<K, V> getNode(Fqn fqn, Flag... flags) {
      setFlagsOnContext(flags);
      try {
         startAtomic();
         try {
            if (exists(fqn))
               return new NodeImpl<K, V>(fqn, cache, batchContainer);
            else return null;
         } finally {
            endAtomic();
         }
      } finally {
         tcc.suspend();
      }
   }

   @Override
   public Node<K, V> getNode(String fqn) {
      return getNode(Fqn.fromString(fqn), null);
   }

   @Override
   public Node<K, V> getNode(String fqn, Flag... flags) {
      return getNode(Fqn.fromString(fqn), flags);
   }

   @Override
   public V get(Fqn fqn, K key) {
      return get(fqn, key, null);
   }

   @Override
   public V get(Fqn fqn, K key, Flag... flags) {
      setFlagsOnContext(flags);
      try {
         Map<K, V> m = getAtomicMap(new NodeKey(fqn, NodeKey.Type.DATA));
         if (m == null) return null;
         return m.get(key);
      } finally {
         tcc.suspend();
      }
   }

   @Override
   public boolean exists(String f) {
      return exists(Fqn.fromString(f), null);
   }

   @Override
   public boolean exists(String fqn, Flag... flags) {
      return exists(Fqn.fromString(fqn), flags);
   }

   @Override
   public boolean exists(Fqn fqn, Flag... flags) {
      tcc.setFlags(flags);
      try {
         return exists(fqn);
      } finally {
         tcc.suspend();
      }
   }

   @Override
   public V get(String fqn, K key) {
      return get(Fqn.fromString(fqn), key, null);
   }

   @Override
   public V get(String fqn, K key, Flag... flags) {
      return get(Fqn.fromString(fqn), key, flags);
   }

   @Override
   public void move(Fqn nodeToMoveFqn, Fqn newParentFqn) throws NodeNotExistsException {
      move(nodeToMoveFqn, newParentFqn, null);
   }

   @Override
   public void move(Fqn nodeToMoveFqn, Fqn newParentFqn, Flag... flags) throws NodeNotExistsException {
      setFlagsOnContext(flags);
      try {
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
      } finally {
         tcc.suspend();
      }
   }

   @Override
   public void move(String nodeToMove, String newParent) throws NodeNotExistsException {
      move(Fqn.fromString(nodeToMove), Fqn.fromString(newParent), null);
   }

   @Override
   public void move(String nodeToMove, String newParent, Flag... flags) throws NodeNotExistsException {
      move(Fqn.fromString(nodeToMove), Fqn.fromString(newParent), flags);
   }

   @Override
   public Map<K, V> getData(Fqn fqn) {
      return getData(fqn, null);
   }

   @Override
   public Map<K, V> getData(Fqn fqn, Flag... flags) {
      setFlagsOnContext(flags);
      try {
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
      } finally {
         tcc.suspend();
      }
   }

   @Override
   public Set<K> getKeys(String fqn) {
      return getKeys(Fqn.fromString(fqn), null);
   }

   @Override
   public Set<K> getKeys(String fqn, Flag... flags) {
      return getKeys(Fqn.fromString(fqn), flags);
   }

   @Override
   public Set<K> getKeys(Fqn fqn) {
      return getKeys(fqn, null);
   }

   @Override
   public Set<K> getKeys(Fqn fqn, Flag... flags) {
      setFlagsOnContext(flags);
      try {
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
      } finally {
         tcc.suspend();
      }
   }

   @Override
   public void clearData(String fqn) {
      clearData(Fqn.fromString(fqn), null);
   }

   @Override
   public void clearData(String fqn, Flag... flags) {
      clearData(Fqn.fromString(fqn), flags);
   }

   @Override
   public void clearData(Fqn fqn) {
      clearData(fqn, null);
   }

   @Override
   public void clearData(Fqn fqn, Flag... flags) {
      setFlagsOnContext(flags);
      try {
         startAtomic();
         try {
            Node<K, V> node = getNode(fqn);
            if (node != null)
               node.clearData();
         } finally {
            endAtomic();
         }
      } finally {
         tcc.suspend();
      }
   }

   @Override
   public V put(Fqn fqn, K key, V value) {
      return put(fqn, key, value, null);
   }

   @Override
   public V put(Fqn fqn, K key, V value, Flag... flags) {
      setFlagsOnContext(flags);
      try {
         startAtomic();
         try {
            createNodeInCache(fqn);
            Map<K, V> m = getAtomicMap(new NodeKey(fqn, NodeKey.Type.DATA));
            return m.put(key, value);
         } finally {
            endAtomic();
         }
      } finally {
         tcc.suspend();
      }
   }

   @Override
   public Cache<?, ?> getCache() {
      // Retrieve the advanced cache as a way to retrieve
      // the cache behind the cache adapter.
      return cache.getAdvancedCache();
   }

   // ------------------ nothing different; just delegate to the cache
   @Override
   public void start() throws CacheException {
      cache.start();
      createRoot();
   }

   @Override
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
