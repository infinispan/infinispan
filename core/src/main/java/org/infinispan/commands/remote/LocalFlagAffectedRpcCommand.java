package org.infinispan.commands.remote;

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
   public boolean hasFlag(Flag flag) {
      return flags != null && flags.contains(flag);
   }
}
