package org.infinispan.commands;

import org.infinispan.context.Flag;

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
}
