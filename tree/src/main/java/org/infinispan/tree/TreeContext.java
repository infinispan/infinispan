package org.infinispan.tree;

import org.infinispan.context.Flag;
import org.infinispan.context.FlagContainer;

import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.Set;

/**
 * Tree invocation context primarily used to hold flags that should be set
 * for all cache operations within a single tree cache operation.
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
   public Set<Flag> getFlags() {
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

   @Override
   public boolean isFlagsUninitialized() {
      return flags == null;
   }
}
