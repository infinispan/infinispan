package org.infinispan.remoting.transport.raft;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.infinispan.commons.io.ByteBuffer;

/**
 * A state machine interface.
 *
 * @since 14.0
 */
public interface RaftStateMachine {

   /**
    * Initializes this instance with the {@link RaftChannel} to be used to send the data.
    *
    * @param raftChannel The {@link RaftChannel} instance.
    */
   void init(RaftChannel raftChannel);

   /**
    * Applies the data from the RAFT protocol.
    * <p>
    * The RAFT protocol ensures that this method is invoked in the same order in all the members.
    *
    * @param buffer The data.
    * @return A {@link ByteBuffer} with the response.
    * @throws Exception If it fails to apply the data in {@link ByteBuffer}.
    */
   ByteBuffer apply(ByteBuffer buffer) throws Exception;

   /**
    * Discards current state and reads the state from {@link DataInput}.
    * <p>
    * There are 2 scenarios where this method may be invoked:
    * <p>
    * 1. when this node starts, it may receive the state from the RAFT leader. 2. when this node starts, it reads the
    * persisted snapshot if available.
    *
    * @param dataInput The {@link DataInput} with the snapshot.
    * @throws IOException If an I/O error happens.
    */
   void readStateFrom(DataInput dataInput) throws IOException;

   /**
    * Writes the current state into the {@link DataOutput}.
    * <p>
    * This method is invoked to truncate the RAFT logs.
    *
    * @param dataOutput The {@link DataOutput} to store the snapshot.
    * @throws IOException If an I/O error happens.
    */
   void writeStateTo(DataOutput dataOutput) throws IOException;

}
