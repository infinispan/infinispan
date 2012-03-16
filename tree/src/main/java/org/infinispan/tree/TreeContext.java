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

import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;

/**
 * Tree invocation context primarily used to hold flags that should be set for all cache operations within a single tree
 * cache operation.
 *
 * @author Galder Zamarreño
 * @since 4.2
 */
public class TreeContext implements FlagContainer {

   private volatile EnumSet<Flag> flags;

   @Override
   public boolean hasFlag(Flag o) {
      return flags != null && flags.contains(o);
   }

   @Override
   public EnumSet<Flag> getFlags() {
      return flags;
   }

   @Override
   public void setFlags(Flag... flags) {
      if (flags == null || flags.length == 0) return;
      if (this.flags == null)
         this.flags = EnumSet.copyOf(Arrays.asList(flags));
      else
         this.flags.addAll(Arrays.asList(flags));
   }

   @Override
   public void setFlags(Collection<Flag> flags) {
      if (flags == null || flags.isEmpty()) return;
      if (this.flags == null)
         this.flags = EnumSet.copyOf(flags);
      else
         this.flags.addAll(flags);
   }

   @Override
   public void reset() {
      flags = null;
   }

   public void addFlag(Flag flag) {
      if (flags == null)
         flags = EnumSet.of(flag);
      else
         flags.add(flag);
   }

   public void addFlags(Flag[] flags) {
      if (flags != null && flags.length != 0) {
         if (this.flags == null) {
            this.flags = EnumSet.noneOf(Flag.class);
            this.flags.addAll(Arrays.asList(flags));
         } else {
            this.flags.addAll(Arrays.asList(flags));
         }
      }
   }

   public void remove(Flag flag) {
      if (flags == null || flags.isEmpty()) return;
      flags.remove(flag);
   }
}
