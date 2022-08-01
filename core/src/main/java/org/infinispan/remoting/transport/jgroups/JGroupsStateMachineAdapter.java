package org.infinispan.remoting.transport.jgroups;

import java.io.DataInput;
import java.io.DataOutput;
import java.util.Objects;

import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.commons.io.ByteBufferImpl;
import org.infinispan.commons.util.Util;
import org.infinispan.remoting.transport.raft.RaftStateMachine;
import org.jgroups.raft.StateMachine;

/**
 * An adapter that converts JGroups {@link StateMachine} calls to {@link  RaftStateMachine}.
 *
 * @since 14.0
 */
class JGroupsStateMachineAdapter<T extends RaftStateMachine> implements StateMachine {

   private final T stateMachine;

   JGroupsStateMachineAdapter(T stateMachine) {
      this.stateMachine = Objects.requireNonNull(stateMachine);
   }

   @Override
   public byte[] apply(byte[] data, int offset, int length, boolean serialize_response) throws Exception {
      // serialize_response ignored.
      ByteBuffer buffer = stateMachine.apply(ByteBufferImpl.create(data, offset, length));
      return buffer == null ? Util.EMPTY_BYTE_ARRAY : buffer.trim();
   }

   @Override
   public void readContentFrom(DataInput in) throws Exception {
      stateMachine.readStateFrom(in);
   }

   @Override
   public void writeContentTo(DataOutput out) throws Exception {
      stateMachine.writeStateTo(out);
   }

   T getStateMachine() {
      return stateMachine;
   }
}
