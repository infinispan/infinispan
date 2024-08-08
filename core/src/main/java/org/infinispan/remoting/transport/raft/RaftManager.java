package org.infinispan.remoting.transport.raft;

import java.util.Collection;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;

import org.infinispan.commons.api.Lifecycle;

/**
 * Use this class to create and register {@link RaftStateMachine}.
 * <p>
 * Each {@link RaftStateMachine} is identified by its name and they are independent of each other; each {@link
 * RaftStateMachine} has it own {@link RaftChannel}.
 *
 * @since 14.0
 */
public interface RaftManager extends Lifecycle {

   /**
    * Register a {@link RaftStateMachine}.
    * <p>
    * If the RAFT protocol is not supported, this method return {@code null}. If a {@link RaftStateMachine} already
    * exists with name {@code channelName}, the existing instance is returned.
    * <p>
    * If {@link #isRaftAvailable()} return {@code false}, this method always returns {@code null}.
    *
    * @param channelName   The name identifying the {@link  RaftStateMachine}.
    * @param supplier      The factory to create a new instance of {@link RaftStateMachine}.
    * @param configuration The {@link RaftChannelConfiguration} for the {@link RaftChannel}.
    * @param <T>           The concrete {@link RaftStateMachine} implementation.
    * @return The {@link RaftStateMachine} instance of {@code null} if unable to create or configure the {@link
    * RaftChannel}.
    */
   <T extends RaftStateMachine> T getOrRegisterStateMachine(String channelName, Supplier<T> supplier, RaftChannelConfiguration configuration);

   /**
    * @return {@code true} if the RAFT protocol is available to be used, {@code false} otherwise.
    */
   boolean isRaftAvailable();

   /**
    * Check if a RAFT leader is elected for the {@link RaftStateMachine} with name {@code channelName}.
    *
    * @param channelName The name identifying the {@link  RaftStateMachine}.
    * @return {@code true} if the leader exists, {@code false} otherwise.
    */
   boolean hasLeader(String channelName);

   /**
    * @return This node raft-id.
    */
   String raftId();

   /**
    * Adds a new member to the RAFT cluster through consensus.
    *
    * <p>
    * All registered {@link RaftStateMachine} are updated with the new membership.
    * <b>Warning:</b> This method is not atomic, the membership might diverge because of failures.
    * </p>
    *
    * @param raftId: A unique name of the new node joining.
    * @return A {@link CompletionStage} that finishes after the operation is applied.
    */
   CompletionStage<Void> addMember(String raftId);

   /**
    * Removes an existing member of the RAFT cluster through consensus.
    *
    * <p>
    * All registered {@link RaftStateMachine} are updated with the new membership.
    * <b>Warning:</b> This method is not atomic, the membership might diverge because of failures.
    * </p>
    *
    * @param raftId: The node name to remove.
    * @return A {@link CompletionStage} that finishes after the operation is applied.
    */
   CompletionStage<Void> removeMembers(String raftId);

   /**
    * The actual members in the cluster.
    *
    * @return An unmodifiable list containing the IDs of all cluster members.
    */
   Collection<String> raftMembers();
}
