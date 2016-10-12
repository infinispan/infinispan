package org.infinispan.commands;

/**
 * Base class for commands that carry topology id.
 *
 * @author Galder Zamarreño
 * @since 5.1
 */
public abstract class AbstractTopologyAffectedCommand extends AbstractFlagAffectedCommand implements TopologyAffectedCommand {

   private int topologyId = -1;

   @Override
   public int getTopologyId() {
      return topologyId;
   }

   @Override
   public void setTopologyId(int topologyId) {
      this.topologyId = topologyId;
   }

}
