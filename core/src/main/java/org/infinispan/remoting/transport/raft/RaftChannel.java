package org.infinispan.remoting.transport.raft;

import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;

import org.infinispan.commons.io.ByteBuffer;

/**
 * A channel abstraction to invoke commands on the RAFT channel.
 *
 * @since 14.0
 */
public interface RaftChannel {

   /**
    * Sends a {@link ByteBuffer} to the RAFT channel to be ordered by the RAFT leader.
    * <p>
    * After the RAFT leader commits, {@link RaftStateMachine#apply(ByteBuffer)} is invoked with the {@link ByteBuffer}
    * and its return value used to complete the {@link CompletionStage}.
    *
    * @param buffer The data to send.
    * @return A {@link CompletionStage} which is completed with the {@link RaftStateMachine#apply(ByteBuffer)} response.
    */
   CompletionStage<ByteBuffer> send(ByteBuffer buffer);

   /**
    * @return The channel name used to register the {@link RaftStateMachine} via {@link
    * RaftManager#getOrRegisterStateMachine(String, Supplier, RaftChannelConfiguration)}.
    */
   String channelName();

   /**
    * @return The node's raft-id.
    */
   String raftId();
}
