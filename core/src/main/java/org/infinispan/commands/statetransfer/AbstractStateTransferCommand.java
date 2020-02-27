package org.infinispan.commands.statetransfer;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import org.infinispan.commands.TopologyAffectedCommand;
import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.IntSetsExternalization;
import org.infinispan.util.ByteString;

/**
 * Base class for commands related to state transfer.
 *
 * @author Ryan Emerson
 * @since 11.0
 */
abstract class AbstractStateTransferCommand extends BaseRpcCommand implements TopologyAffectedCommand {

   private final byte commandId;
   protected int topologyId;
   protected IntSet segments;

   AbstractStateTransferCommand(byte commandId, ByteString cacheName) {
      super(cacheName);
      this.commandId = commandId;
   }

   AbstractStateTransferCommand(byte commandId, ByteString cacheName, int topologyId, IntSet segments) {
      this(commandId, cacheName);
      this.topologyId = topologyId;
      this.segments = segments;
   }

   @Override
   public boolean isReturnValueExpected() {
      return true;
   }

   @Override
   public int getTopologyId() {
      return topologyId;
   }

   @Override
   public void setTopologyId(int topologyId) {
      this.topologyId = topologyId;
   }

   public IntSet getSegments() {
      return segments;
   }

   @Override
   public byte getCommandId() {
      return commandId;
   }

   @Override
   public void writeTo(ObjectOutput output) throws IOException {
      IntSetsExternalization.writeTo(output, segments);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      segments = IntSetsExternalization.readFrom(input);
   }
}
