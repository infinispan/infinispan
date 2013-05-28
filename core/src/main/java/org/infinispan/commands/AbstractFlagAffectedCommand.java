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
public abstract class AbstractFlagAffectedCommand extends AbstractLocalFlagAffectedCommand implements FlagAffectedCommand {

   private int topologyId = -1;

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
