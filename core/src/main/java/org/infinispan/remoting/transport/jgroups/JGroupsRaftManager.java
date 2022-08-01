package org.infinispan.remoting.transport.jgroups;

import java.nio.file.Path;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.commons.io.ByteBufferImpl;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.remoting.transport.raft.RaftChannel;
import org.infinispan.remoting.transport.raft.RaftChannelConfiguration;
import org.infinispan.remoting.transport.raft.RaftManager;
import org.infinispan.remoting.transport.raft.RaftStateMachine;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.jgroups.JChannel;
import org.jgroups.fork.ForkChannel;
import org.jgroups.protocols.FORK;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.raft.ELECTION;
import org.jgroups.protocols.raft.FileBasedLog;
import org.jgroups.protocols.raft.InMemoryLog;
import org.jgroups.protocols.raft.NO_DUPES;
import org.jgroups.protocols.raft.RAFT;
import org.jgroups.protocols.raft.REDIRECT;
import org.jgroups.raft.RaftHandle;
import org.jgroups.raft.StateMachine;
import org.jgroups.stack.ProtocolStack;

/**
 * A {@link RaftManager} implementation that use JGroups-RAFT protocol.
 * <p>
 * This implementation uses multiple {@link ForkChannel}, one of each {@link RaftStateMachine} registered, and it
 * expects the {@link  FORK} protocol to be present in the main {@link  JChannel}.
 *
 * @since 14.0
 */
class JGroupsRaftManager implements RaftManager {

   private static final Log log = LogFactory.getLog(JGroupsRaftManager.class);

   private final JChannel mainChannel;
   private final Collection<String> raftMembers;
   private final String raftId;
   private final String persistenceDirectory;
   private final Map<String, JgroupsRaftChannel<? extends RaftStateMachine>> raftStateMachineMap = new ConcurrentHashMap<>(16);

   JGroupsRaftManager(GlobalConfiguration globalConfiguration, JChannel mainChannel) {
      if (JGroupsTransport.findFork(mainChannel) == null) {
         throw log.forkProtocolRequired();
      }
      this.mainChannel = mainChannel;
      raftMembers = globalConfiguration.transport().raftMembers();
      raftId = globalConfiguration.transport().nodeName();
      persistenceDirectory = globalConfiguration.globalState().enabled() ?
            globalConfiguration.globalState().persistentLocation() :
            null;
   }

   @Override
   public <T extends RaftStateMachine> T getOrRegisterStateMachine(String channelName, Supplier<T> supplier, RaftChannelConfiguration configuration) {
      Objects.requireNonNull(channelName);
      Objects.requireNonNull(supplier);
      Objects.requireNonNull(configuration);
      //noinspection unchecked
      JgroupsRaftChannel<T> raftChannel = (JgroupsRaftChannel<T>) raftStateMachineMap.computeIfAbsent(channelName, s -> createRaftChannel(s, configuration, supplier));
      return raftChannel == null ? null : raftChannel.stateMachine();
   }

   @Override
   public boolean isRaftAvailable() {
      return true;
   }

   @Override
   public boolean hasLeader(String channelName) {
      JgroupsRaftChannel<?> raftChannel = raftStateMachineMap.get(channelName);
      return raftChannel != null && raftChannel.raftHandle.leader() != null;
   }

   @Override
   public String raftId() {
      return raftId;
   }

   private <T extends RaftStateMachine> JgroupsRaftChannel<T> createRaftChannel(String name, RaftChannelConfiguration configuration, Supplier<? extends T> supplier) {
      ForkChannel forkChannel = null;
      try {
         forkChannel = createForkChannel(name, configuration);
         forkChannel.connect(name);
      } catch (Exception e) {
         log.errorCreatingForkChannel(name, e);
         if (forkChannel != null) {
            // disconnect removes channel from FORK protocol
            forkChannel.disconnect();
         } else {
            JGroupsTransport.findFork(mainChannel).remove(name);
         }
         return null;
      }
      T stateMachine = supplier.get();
      JgroupsRaftChannel<T> raftChannel = new JgroupsRaftChannel<>(name, forkChannel, stateMachine);
      stateMachine.init(raftChannel);
      return raftChannel;
   }

   private ForkChannel createForkChannel(String name, RaftChannelConfiguration configuration) throws Exception {
      RAFT raftProtocol = new RAFT();
      switch (configuration.logMode()) {
         case VOLATILE:
            raftProtocol
                  .logClass(InMemoryLog.class.getCanonicalName())
                  .logPrefix(name + "-" + raftId);
            break;
         case PERSISTENT:
            if (persistenceDirectory == null) {
               throw log.raftGlobalStateDisabled();
            }
            raftProtocol
                  .logClass(FileBasedLog.class.getCanonicalName())
                  .logPrefix(Path.of(persistenceDirectory, name, raftId).toAbsolutePath().toString());
            break;
         default:
            throw new IllegalStateException();
      }
      raftProtocol
            .members(raftMembers)
            .raftId(raftId);

      return new ForkChannel(mainChannel, name, name, new ELECTION(), raftProtocol, new REDIRECT());
   }

   /**
    * Inserts the {@link NO_DUPES} protocol in the main {@link  JChannel}.
    * <p>
    * The {@link NO_DUPES} protocol must be set in the main {@link JChannel} below {@link GMS} to detect and reject any
    * duplicated members with the same "raft-id".
    */
   @Override
   public void start() {
      // NO_DUPES need to be on the main channel
      ProtocolStack protocolStack = mainChannel.getProtocolStack();
      if (protocolStack.findProtocol(NO_DUPES.class) != null) {
         // already in main channel
         return;
      }
      GMS gms = protocolStack.findProtocol(GMS.class);
      if (gms == null) {
         // GMS not found, unable to use NO_DUPES
         return;
      }
      protocolStack.insertProtocolInStack(new NO_DUPES(), gms, ProtocolStack.Position.BELOW);
   }

   @Override
   public void stop() {
      raftStateMachineMap.values().forEach(JgroupsRaftChannel::disconnect);
      raftStateMachineMap.clear();
   }

   private static class JgroupsRaftChannel<T extends RaftStateMachine> implements RaftChannel {

      private final RaftHandle raftHandle;
      private final String channelName;
      private final JChannel forkedChannel;

      JgroupsRaftChannel(String channelName, JChannel forkedChannel, RaftStateMachine stateMachine) {
         this.channelName = channelName;
         this.forkedChannel = forkedChannel;
         raftHandle = new RaftHandle(forkedChannel, new JGroupsStateMachineAdapter<>(stateMachine));
      }

      @Override
      public CompletionStage<ByteBuffer> send(ByteBuffer buffer) {
         try {
            return raftHandle.setAsync(buffer.getBuf(), buffer.getOffset(), buffer.getLength()).thenApply(ByteBufferImpl::create);
         } catch (Exception e) {
            return CompletableFutures.completedExceptionFuture(e);
         }
      }

      @Override
      public String channelName() {
         return channelName;
      }

      @Override
      public String raftId() {
         return raftHandle.raftId();
      }

      T stateMachine() {
         StateMachine stateMachine = raftHandle.stateMachine();
         assert stateMachine instanceof JGroupsStateMachineAdapter;
         //noinspection unchecked
         return ((JGroupsStateMachineAdapter<T>) stateMachine).getStateMachine();
      }

      void disconnect() {
         // removes the forked channel from FORK
         forkedChannel.disconnect();
      }
   }

}
