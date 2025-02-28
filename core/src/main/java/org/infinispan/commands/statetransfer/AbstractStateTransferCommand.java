package org.infinispan.commands.statetransfer;

import org.infinispan.commands.TopologyAffectedCommand;
import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.commons.util.IntSet;
import org.infinispan.marshall.protostream.impl.WrappedMessages;
import org.infinispan.protostream.WrappedMessage;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.util.ByteString;

/**
 * Base class for commands related to state transfer.
 *
 * @author Ryan Emerson
 * @since 11.0
 */
abstract class AbstractStateTransferCommand extends BaseRpcCommand implements StateTransferCommand, TopologyAffectedCommand {

   protected int topologyId;
   protected IntSet segments;

   AbstractStateTransferCommand(ByteString cacheName) {
      super(cacheName);
   }

   AbstractStateTransferCommand(ByteString cacheName, int topologyId, IntSet segments) {
      this(cacheName);
      this.topologyId = topologyId;
      this.segments = segments;
   }

   @Override
   @ProtoField(2)
   public int getTopologyId() {
      return topologyId;
   }

   @ProtoField(3)
   WrappedMessage getWrappedSegments() {
      return WrappedMessages.orElseNull(segments);
   }

   @Override
   public boolean isReturnValueExpected() {
      return true;
   }

   @Override
   public void setTopologyId(int topologyId) {
      this.topologyId = topologyId;
   }

   public IntSet getSegments() {
      return segments;
   }

}
