package org.infinispan.commands;

import org.infinispan.context.Flag;
import org.infinispan.metadata.Metadata;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.Set;

/**
 * Base class for those commands that can carry flags.
 *
 * @author Galder Zamarreño
 * @since 5.1
 */
public abstract class AbstractFlagAffectedCommand implements FlagAffectedCommand, TopologyAffectedCommand {

   protected Set<Flag> flags;

   private int topologyId = -1;

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

   @Override
   public int getTopologyId() {
      return topologyId;
   }

   @Override
   public void setTopologyId(int topologyId) {
      this.topologyId = topologyId;
   }

   @Override
   public Metadata getMetadata() {
      return null;
   }

   @Override
   public void setMetadata(Metadata metadata) {
      // no-op
   }

}
