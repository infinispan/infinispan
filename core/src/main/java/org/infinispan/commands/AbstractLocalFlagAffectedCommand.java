package org.infinispan.commands;

import org.infinispan.context.Flag;
import org.infinispan.metadata.Metadata;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.Set;

/**
 * Base class for those local commands that can carry flags.
 *
 * @author William Burns
 * @since 6.0
 */
public abstract class AbstractLocalFlagAffectedCommand implements LocalFlagAffectedCommand {

   protected Set<Flag> flags;

   @Override
   public Set<Flag> getFlags() {
      return flags;
   }

   @Override
   public void setFlags(Set<Flag> flags) {
      this.flags = flags;
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
   public boolean hasFlag(Flag flag) {
      return flags != null && flags.contains(flag);
   }
}
