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
import org.infinispan.atomic.AtomicHashMapProxy;
import org.infinispan.atomic.AtomicMap;
import org.infinispan.batch.BatchContainer;
import org.infinispan.context.Flag;
import org.infinispan.util.Immutables;
import org.infinispan.util.Util;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Implementation backed by an {@link AtomicMap}
 *
 * @author Manik Surtani (<a href="mailto:manik AT jboss DOT org">manik AT jboss DOT org</a>)
 * @since 4.0
 */
public class NodeImpl<K, V> extends TreeStructureSupport implements Node<K, V> {
   Fqn fqn;
   NodeKey dataKey, structureKey;

   public NodeImpl(Fqn fqn, AdvancedCache<?, ?> cache, BatchContainer batchContainer) {
      super(cache, batchContainer);
      this.fqn = fqn;
      dataKey = new NodeKey(fqn, NodeKey.Type.DATA);
      structureKey = new NodeKey(fqn, NodeKey.Type.STRUCTURE);
   }

   public Node<K, V> getParent() {
      if (fqn.isRoot()) return this;
      return new NodeImpl<K, V>(fqn.getParent(), cache, batchContainer);
   }

   public Node<K, V> getParent(Flag... flags) {
      tcc.createTreeContext().addFlags(flags);
      return getParent();
   }

   public Set<Node<K, V>> getChildren() {
      startAtomic();
      try {
         Set<Node<K, V>> result = new HashSet<Node<K, V>>();
         for (Fqn f : getStructure().values()) {
            NodeImpl<K, V> n = new NodeImpl<K, V>(f, cache, batchContainer);
            result.add(n);
         }
         return Immutables.immutableSetWrap(result);
      }
      finally {
         endAtomic();
      }
   }

   public Set<Node<K, V>> getChildren(Flag... flags) {
      tcc.createTreeContext().addFlags(flags);
      return getChildren();
   }

   public Set<Object> getChildrenNames() {
      return Immutables.immutableSetCopy(getStructure().keySet());
   }

   public Set<Object> getChildrenNames(Flag... flags) {
      tcc.createTreeContext().addFlags(flags);
      return getChildrenNames();
   }

   @SuppressWarnings("unchecked")
   public Map<K, V> getData() {
      return Collections.unmodifiableMap(new HashMap(getDataInternal()));
   }

   public Map<K, V> getData(Flag... flags) {
      tcc.createTreeContext().addFlags(flags);
      return getData();
   }

   public Set<K> getKeys() {
      startAtomic();
      try {
         return getData().keySet();
      }
      finally {
         endAtomic();
      }
   }

   public Set<K> getKeys(Flag... flags) {
      tcc.createTreeContext().addFlags(flags);
      return getKeys();
   }

   public Fqn getFqn() {
      return fqn;
   }

   public Node<K, V> addChild(Fqn f) {
      startAtomic();
      try {
         Fqn absoluteChildFqn = Fqn.fromRelativeFqn(fqn, f);

         //1) first register it with the parent
         AtomicMap<Object, Fqn> structureMap = getStructure();
         structureMap.put(f.getLastElement(), absoluteChildFqn);

         //2) then create the structure and data maps
         createNodeInCache(absoluteChildFqn);

         return new NodeImpl<K, V>(absoluteChildFqn, cache, batchContainer);
      }
      finally {
         endAtomic();
      }
   }

   public Node<K, V> addChild(Fqn f, Flag... flags) {
      tcc.createTreeContext().addFlags(flags);
      return addChild(f);
   }

   public boolean removeChild(Fqn f) {
      return removeChild(f.getLastElement());
   }

   public boolean removeChild(Fqn f, Flag... flags) {
      tcc.createTreeContext().addFlags(flags);
      return removeChild(f);
   }

   public boolean removeChild(Object childName) {
      startAtomic();
      try {
         AtomicMap<Object, Fqn> s = getStructure();
         Fqn childFqn = s.remove(childName);
         if (childFqn != null) {
            Node<K, V> child = new NodeImpl<K, V>(childFqn, cache, batchContainer);
            child.removeChildren();
            child.clearData();  // this is necessary in case we have a remove and then an add on the same node, in the same tx.
            cache.remove(new NodeKey(childFqn, NodeKey.Type.DATA));
            cache.remove(new NodeKey(childFqn, NodeKey.Type.STRUCTURE));
            return true;
         }

         return false;
      }
      finally {
         endAtomic();
      }
   }

   public boolean removeChild(Object childName, Flag... flags) {
      tcc.createTreeContext().addFlags(flags);
      return removeChild(childName);
   }

   public Node<K, V> getChild(Fqn f) {
      startAtomic();
      try {
         if (hasChild(f))
            return new NodeImpl<K, V>(Fqn.fromRelativeFqn(fqn, f), cache, batchContainer);
         else
            return null;
      }
      finally {
         endAtomic();
      }
   }

   public Node<K, V> getChild(Fqn f, Flag... flags) {
      tcc.createTreeContext().addFlags(flags);
      return getChild(f);
   }

   public Node<K, V> getChild(Object name) {
      startAtomic();
      try {
         if (hasChild(name))
            return new NodeImpl<K, V>(Fqn.fromRelativeElements(fqn, name), cache, batchContainer);
         else
            return null;
      }
      finally {
         endAtomic();
      }
   }

   public Node<K, V> getChild(Object name, Flag... flags) {
      tcc.createTreeContext().addFlags(flags);
      return getChild(name);
   }

   public V put(K key, V value) {
      startAtomic();
      try {
         AtomicHashMapProxy<K, V> map = (AtomicHashMapProxy<K, V>) getDataInternal();
         return map.put(key, value);
      }
      finally {
         endAtomic();
      }
   }

   public V put(K key, V value, Flag... flags) {
      tcc.createTreeContext().addFlags(flags);
      return put(key, value);
   }

   public V putIfAbsent(K key, V value) {
      startAtomic();
      try {
         AtomicMap<K, V> data = getDataInternal();
         if (!data.containsKey(key)) return data.put(key, value);

         return null;
      }
      finally {
         endAtomic();
      }
   }

   public V putIfAbsent(K key, V value, Flag... flags) {
      tcc.createTreeContext().addFlags(flags);
      return putIfAbsent(key, value);
   }

   public V replace(K key, V value) {
      startAtomic();
      try {
         AtomicMap<K, V> map = getAtomicMap(dataKey);
         if (map.containsKey(key))
            return map.put(key, value);
         else
            return null;
      }
      finally {
         endAtomic();
      }
   }

   public V replace(K key, V value, Flag... flags) {
      tcc.createTreeContext().addFlags(flags);
      return replace(key, value);
   }

   public boolean replace(K key, V oldValue, V newValue) {
      startAtomic();
      try {
         AtomicMap<K, V> data = getDataInternal();
         V old = data.get(key);
         if (Util.safeEquals(oldValue, old)) {
            data.put(key, newValue);
            return true;
         }
         return false;
      }
      finally {
         endAtomic();
      }
   }

   public boolean replace(K key, V oldValue, V value, Flag... flags) {
      tcc.createTreeContext().addFlags(flags);
      return replace(key, oldValue, value);
   }

   public void putAll(Map<? extends K, ? extends V> map) {
      startAtomic();
      try {
         getDataInternal().putAll(map);
      }
      finally {
         endAtomic();
      }
   }

   public void putAll(Map<? extends K, ? extends V> map, Flag... flags) {
      tcc.createTreeContext().addFlags(flags);
      putAll(map);
   }

   public void replaceAll(Map<? extends K, ? extends V> map) {
      startAtomic();
      try {
         AtomicMap<K, V> data = getDataInternal();
         data.clear();
         data.putAll(map);
      }
      finally {
         endAtomic();
      }
   }

   public void replaceAll(Map<? extends K, ? extends V> map, Flag... flags) {
      tcc.createTreeContext().addFlags(flags);
      replaceAll(map);
   }

   public V get(K key) {
      return getData().get(key);
   }

   public V get(K key, Flag... flags) {
      tcc.createTreeContext().addFlags(flags);
      return get(key);
   }

   public V remove(K key) {
      startAtomic();
      try {
         return getDataInternal().remove(key);
      }
      finally {
         endAtomic();
      }
   }

   public V remove(K key, Flag... flags) {
      tcc.createTreeContext().addFlags(flags);
      return remove(key);
   }

   public void clearData() {
      getDataInternal().clear();
   }

   public void clearData(Flag... flags) {
      tcc.createTreeContext().addFlags(flags);
      clearData();
   }

   public int dataSize() {
      return getData().size();
   }

   public int dataSize(Flag... flags) {
      tcc.createTreeContext().addFlags(flags);
      return dataSize();
   }

   public boolean hasChild(Fqn f) {
      if (f.size() > 1) {
         // indirect child.
         Fqn absoluteFqn = Fqn.fromRelativeFqn(fqn, f);
         return exists(absoluteFqn);
      } else {
         return hasChild(f.getLastElement());
      }
   }

   public boolean hasChild(Fqn f, Flag... flags) {
      tcc.createTreeContext().addFlags(flags);
      return hasChild(f);
   }

   public boolean hasChild(Object o) {
      return getStructure().containsKey(o);
   }

   public boolean hasChild(Object o, Flag... flags) {
      tcc.createTreeContext().addFlags(flags);
      return hasChild(o);
   }

   public boolean isValid() {
      return cache.containsKey(dataKey);
   }

   public void removeChildren() {
      startAtomic();
      try {
         Map<Object, Fqn> s = getStructure();
         for (Object o : Immutables.immutableSetCopy(s.keySet())) removeChild(o);
      }
      finally {
         endAtomic();
      }
   }

   public void removeChildren(Flag... flags) {
      tcc.createTreeContext().addFlags(flags);
      removeChildren();
   }

   @SuppressWarnings("unchecked")
   AtomicMap<K, V> getDataInternal() {
      return getAtomicMap(dataKey);
   }

   @SuppressWarnings("unchecked")
   AtomicMap<Object, Fqn> getStructure() {
      return getAtomicMap(structureKey);
   }

   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      NodeImpl<?, ?> node = (NodeImpl<?, ?>) o;

      if (fqn != null ? !fqn.equals(node.fqn) : node.fqn != null) return false;

      return true;
   }

   public int hashCode() {
      return (fqn != null ? fqn.hashCode() : 0);
   }

   @Override
   public String toString() {
      return "NodeImpl{" +
            "fqn=" + fqn +
            '}';
   }
}
