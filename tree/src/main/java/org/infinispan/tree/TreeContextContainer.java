/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other
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

import org.infinispan.context.Flag;
import org.infinispan.context.FlagContainer;

import java.util.Collection;
import java.util.EnumSet;
import java.util.Set;

/**
 * Invocation context container holding tree invocation context for the current thread.
 *
 * @author Galder Zamarreño
 * @since 4.2
 */
public class TreeContextContainer implements FlagContainer {

   private final ThreadLocal<TreeContext> tcTL = new ThreadLocal<TreeContext>();

   public TreeContext createTreeContext() {
      TreeContext existing = tcTL.get();
      if (existing == null) {
         TreeContext treeContext = new TreeContext();
         tcTL.set(treeContext);
         return treeContext;
      } else {
         return existing;
      }
   }

   public TreeContext getTreeContext() {
      return tcTL.get();
   }

   public TreeContext suspend() {
      TreeContext treeContext = tcTL.get();
      tcTL.remove();
      return treeContext;
   }

   @Override
   public boolean hasFlag(Flag o) {
      TreeContext tc = tcTL.get();
      return tc != null && tc.hasFlag(o);
   }

   @Override
   public Set<Flag> getFlags() {
      TreeContext treeContext = tcTL.get();
      return treeContext == null ? EnumSet.noneOf(Flag.class) : treeContext.getFlags();
   }

   @Override
   public void setFlags(Flag... flags) {
      createTreeContext().setFlags(flags);
   }

   @Override
   public void setFlags(Collection<Flag> flags) {
      createTreeContext().setFlags(flags);
   }

   @Override
   public void reset() {
      TreeContext treeContext = tcTL.get();
      if (treeContext != null) treeContext.reset();
   }

   public void remove(Flag flag) {
      TreeContext treeContext = tcTL.get();
      if (treeContext != null) treeContext.remove(flag);
   }
}
