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
package org.horizon.tree;

import org.horizon.Cache;
import org.horizon.atomic.AtomicMap;
import org.horizon.atomic.AtomicMapCache;
import org.horizon.batch.AutoBatchSupport;
import org.horizon.batch.BatchContainer;
import org.horizon.invocation.InvocationContextContainer;
import org.horizon.invocation.Flag;
import org.horizon.lock.LockManager;
import org.horizon.logging.Log;
import org.horizon.logging.LogFactory;

public class TreeStructureSupport extends AutoBatchSupport {
   private static Log log = LogFactory.getLog(TreeStructureSupport.class);

   AtomicMapCache cache;
   InvocationContextContainer icc;

   public TreeStructureSupport(Cache cache, BatchContainer batchContainer, InvocationContextContainer icc) {
      this.cache = (AtomicMapCache) cache;
      this.batchContainer = batchContainer;
      this.icc = icc;
   }

   public boolean exists(Fqn f) {
      startAtomic();
      try {
         return cache.containsKey(new NodeKey(f, NodeKey.Type.DATA)) && cache.containsKey(new NodeKey(f, NodeKey.Type.STRUCTURE));
      }
      finally {
         endAtomic();
      }
   }

   /**
    * @param fqn
    * @return true if created, false if this was not necessary
    */
   boolean createNodeInCache(Fqn fqn) {
      startAtomic();
      try {
         NodeKey dataKey = new NodeKey(fqn, NodeKey.Type.DATA);
         NodeKey structureKey = new NodeKey(fqn, NodeKey.Type.STRUCTURE);
         if (cache.containsKey(dataKey) && cache.containsKey(structureKey)) return false;
         Fqn parent = fqn.getParent();
         if (!fqn.isRoot()) {
            if (!exists(parent)) createNodeInCache(parent);
            AtomicMap<Object, Fqn> parentStructure = getStructure(parent);
            // don't lock parents for child insert/removes!
            icc.get().setFlags(Flag.SKIP_LOCKING);
            parentStructure.put(fqn.getLastElement(), fqn);
         }
         cache.getAtomicMap(structureKey);
         cache.getAtomicMap(dataKey);
         if (log.isTraceEnabled()) log.trace("Created node " + fqn);
         return true;
      }
      finally {
         endAtomic();
      }
   }

   AtomicMap<Object, Fqn> getStructure(Fqn fqn) {
      return cache.getAtomicMap(new NodeKey(fqn, NodeKey.Type.STRUCTURE), Object.class, Fqn.class);
   }

//   void updateStructure(Fqn fqn, FastCopyHashMap<Object, Fqn> structure)
//   {
//      cache.put(new NodeKey(fqn, NodeKey.Type.STRUCTURE), structure.clone());
//   }

   public static boolean isLocked(Cache c, LockManager lockManager, Fqn fqn) {
      return lockManager.isLocked(new NodeKey(fqn, NodeKey.Type.STRUCTURE)) &&
            lockManager.isLocked(new NodeKey(fqn, NodeKey.Type.DATA));
   }

   /**
    * Visual representation of a tree
    *
    * @param cache cache to dump
    * @return String rep
    */
   public static String printTree(TreeCache<?, ?> cache, boolean details) {
      StringBuilder sb = new StringBuilder();
//      sb.append("Raw keys: " + cache.getCache().keySet());
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
}
