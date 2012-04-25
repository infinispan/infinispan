/*
 * Copyright 2012 Red Hat, Inc. and/or its affiliates.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA
 */

package org.infinispan.tree;

import org.infinispan.AdvancedCache;
import org.infinispan.context.Flag;

import java.util.Arrays;
import java.util.EnumSet;

public class DecoratedCacheAdapter<K, V> extends CacheAdapter<K, V> {

   private final EnumSet<Flag> flags = EnumSet.noneOf(Flag.class);

   DecoratedCacheAdapter(AdvancedCache advancedCache, TreeContextContainer tcc, Flag... flags) {
      super(advancedCache, tcc);
      if (flags != null && flags.length > 0) this.flags.addAll(Arrays.asList(flags));
   }

   @Override
   protected EnumSet<Flag> getFlags() {
      if (tcc.getTreeContext() == null)
         return flags;
      else {
         EnumSet<Flag> ctxFlags = tcc.getTreeContext().getFlags();
         if (!ctxFlags.isEmpty()) {
            EnumSet<Flag> copy = ctxFlags.clone();
            copy.addAll(flags);
            return copy;
         } else {
            return flags;
         }
      }
   }

   @Override
   public AdvancedCache<K, V> withFlags(Flag... flags) {
      this.flags.addAll(Arrays.asList(flags));
      return this;
   }
}
