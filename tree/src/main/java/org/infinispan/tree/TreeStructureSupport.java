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
import org.infinispan.atomic.AtomicMap;
import org.infinispan.atomic.AtomicMapLookup;
import org.infinispan.batch.AutoBatchSupport;
import org.infinispan.batch.BatchContainer;
import org.infinispan.util.concurrent.locks.LockManager;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

public class TreeStructureSupport extends AutoBatchSupport {
   private static final Log log = LogFactory.getLog(TreeStructureSupport.class);

   protected final AdvancedCache<NodeKey, AtomicMap<?, ?>> cache;

   @SuppressWarnings("unchecked")
   public TreeStructureSupport(AdvancedCache<?, ?> cache, BatchContainer batchContainer) {
      this.cache = (AdvancedCache<NodeKey, AtomicMap<?, ?>>) cache;
      this.batchContainer = batchContainer;
   }

   public boolean exists(Fqn f) {
      return exists(cache, f);
   }

   protected boolean exists(AdvancedCache<?, ?> cache, Fqn f) {
      startAtomic();
      try {
         return cache.containsKey(new NodeKey(f, NodeKey.Type.DATA))
               && cache.containsKey(new NodeKey(f, NodeKey.Type.STRUCTURE));
      }
      finally {
         endAtomic();
      }
   }

   /**
    * @return true if created, false if this was not necessary.
    */
   boolean createNodeInCache(Fqn fqn) {
      return createNodeInCache(cache, fqn);
   }

   protected boolean createNodeInCache(AdvancedCache<?, ?> cache, Fqn fqn) {
      startAtomic();
      try {
         NodeKey dataKey = new NodeKey(fqn, NodeKey.Type.DATA);
         NodeKey structureKey = new NodeKey(fqn, NodeKey.Type.STRUCTURE);
         if (cache.containsKey(dataKey) && cache.containsKey(structureKey)) return false;
         Fqn parent = fqn.getParent();
         if (!fqn.isRoot()) {
            if (!exists(cache, parent)) createNodeInCache(cache, parent);
            AtomicMap<Object, Fqn> parentStructure = getStructure(cache, parent);
            parentStructure.put(fqn.getLastElement(), fqn);
         }
         getAtomicMap(cache, structureKey);
         getAtomicMap(cache, dataKey);
         if (log.isTraceEnabled()) log.tracef("Created node %s", fqn);
         return true;
      }
      finally {
         endAtomic();
      }
   }

   private AtomicMap<Object, Fqn> getStructure(AdvancedCache<?, ?> cache, Fqn fqn) {
      return getAtomicMap(cache, new NodeKey(fqn, NodeKey.Type.STRUCTURE));
   }

   public static boolean isLocked(LockManager lockManager, Fqn fqn) {
      return ((lockManager.isLocked(new NodeKey(fqn, NodeKey.Type.STRUCTURE)) &&
            lockManager.isLocked(new NodeKey(fqn, NodeKey.Type.DATA))));
   }

   /**
    * Returns a String representation of a tree cache.
    */
   public static String printTree(TreeCache<?, ?> cache, boolean details) {
      StringBuilder sb = new StringBuilder();
      sb.append("\n\n");

      // walk tree
      sb.append("+ ").append(Fqn.SEPARATOR);
      if (details) sb.append("  ").append(cache.getRoot().getData());
      sb.append("\n");
      addChildren(cache.getRoot(), 1, sb, details);
      return sb.toString();
   }

   private static void addChildren(Node<?, ?> node, int depth, StringBuilder sb, boolean details) {
      for (Node<?, ?> child : node.getChildren()) {
         for (int i = 0; i < depth; i++) sb.append("  "); // indentations
         sb.append("+ ");
         sb.append(child.getFqn().getLastElementAsString()).append(Fqn.SEPARATOR);
         if (details) sb.append("  ").append(child.getData());
         sb.append("\n");
         addChildren(child, depth + 1, sb, details);
      }
   }

   protected final <K, V> AtomicMap<K, V> getAtomicMap(NodeKey key) {
      return AtomicMapLookup.getAtomicMap(cache, key);
   }

   protected final <K, V> AtomicMap<K, V> getAtomicMap(AdvancedCache<?, ?> cache, NodeKey key) {
      return AtomicMapLookup.getAtomicMap((AdvancedCache<NodeKey, AtomicMap<?, ?>>) cache, key);
   }

}
