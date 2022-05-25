package org.infinispan.remoting.transport.impl;

import java.util.function.Supplier;

import org.infinispan.remoting.transport.Transport;
import org.infinispan.remoting.transport.raft.RaftChannelConfiguration;
import org.infinispan.remoting.transport.raft.RaftManager;
import org.infinispan.remoting.transport.raft.RaftStateMachine;

/**
 * A NO-OP implementation of {@link RaftManager}.
 * <p>
 * This implementation is used when RAFT is not supported by the {@link Transport}.
 *
 * @since 14.0
 */
public enum EmptyRaftManager implements RaftManager {
   INSTANCE;


   @Override
   public <T extends RaftStateMachine> T getOrRegisterStateMachine(String channelName, Supplier<T> supplier, RaftChannelConfiguration configuration) {
      return null;
   }

   @Override
   public boolean isRaftAvailable() {
      return false;
   }

   @Override
   public boolean hasLeader(String channelName) {
      return false;
   }

   @Override
   public String raftId() {
      return null;
   }

   @Override
   public void start() {

   }

   @Override
   public void stop() {

   }
}
