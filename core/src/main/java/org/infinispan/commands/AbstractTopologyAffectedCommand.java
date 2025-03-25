package org.infinispan.commands;

import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.util.ByteString;

/**
 * Base class for commands that carry topology id.
 *
 * @author Galder Zamarreño
 * @since 5.1
 */
public abstract class AbstractTopologyAffectedCommand extends AbstractFlagAffectedCommand implements TopologyAffectedCommand {

   protected int topologyId;

   protected AbstractTopologyAffectedCommand(ByteString cacheName, long flags, int topologyId) {
      super(cacheName, flags);
      this.topologyId = topologyId;
   }

   protected AbstractTopologyAffectedCommand(long flags, int topologyId) {
      super(flags);
      this.topologyId = topologyId;
   }

   @Override
   @ProtoField(3)
   public int getTopologyId() {
      return topologyId;
   }

   @Override
   public void setTopologyId(int topologyId) {
      this.topologyId = topologyId;
   }
}
