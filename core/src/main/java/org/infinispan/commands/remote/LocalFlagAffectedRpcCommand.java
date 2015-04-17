package org.infinispan.commands.remote;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.Set;

import org.infinispan.commands.LocalFlagAffectedCommand;
import org.infinispan.context.Flag;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public abstract class LocalFlagAffectedRpcCommand extends BaseRpcCommand implements LocalFlagAffectedCommand {
   protected Set<Flag> flags;

   protected LocalFlagAffectedRpcCommand(String cacheName, Set<Flag> flags) {
      super(cacheName);
      this.flags = flags;
   }

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
