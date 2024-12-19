package org.infinispan.commands.triangle;

import static org.infinispan.commands.write.ValueMatcher.MATCH_ALWAYS;

import java.util.concurrent.CompletionStage;

import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.InvocationContextFactory;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.interceptors.AsyncInterceptorChain;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.util.ByteString;
import org.infinispan.commons.util.concurrent.CompletableFutures;

/**
 * A write operation sent from the primary owner to the backup owners.
 * <p>
 * This is a base command with the {@link CommandInvocationId}, topology and flags.
 * <p>
 * Since the primary &rarr; backup operations are ordered by segment, it contains the segment to be updated and its
 * sequence number.
 *
 * @author Pedro Ruivo
 * @since 9.2
 */
public abstract class BackupWriteCommand extends BaseRpcCommand {

   //common attributes of all write commands
   @ProtoField(number = 2)
   final CommandInvocationId commandInvocationId;

   @ProtoField(number = 3, defaultValue = "-1")
   final int topologyId;

   @ProtoField(number = 4, defaultValue = "0")
   final long flags;

   //backup commands are ordered by segment. this is the sequence number of the segment.
   @ProtoField(number = 5, defaultValue = "-1")
   final long sequence;

   @ProtoField(number = 6, defaultValue = "-1")
   final int segmentId;

   protected BackupWriteCommand(ByteString cacheName, WriteCommand command, long sequence, int segmentId) {
      super(cacheName);
      this.commandInvocationId = command.getCommandInvocationId();
      this.topologyId = command.getTopologyId();
      this.flags = command.getFlagsBitSet();
      this.sequence = sequence;
      this.segmentId = segmentId;
   }

   // Used by ProtoFactory implementations
   protected BackupWriteCommand(ByteString cacheName, CommandInvocationId commandInvocationId, int topologyId,
                                long flags, long sequence, int segmentId) {
      super(cacheName);
      this.commandInvocationId = commandInvocationId;
      this.topologyId = topologyId;
      this.flags = flags;
      this.sequence = sequence;
      this.segmentId = segmentId;
   }

   @Override
   public final CompletionStage<?> invokeAsync(ComponentRegistry componentRegistry) {
      WriteCommand command = createWriteCommand();
      if (command == null) {
         // No-op command
         return CompletableFutures.completedNull();
      }
      command.init(componentRegistry);
      command.setFlagsBitSet(flags);
      // Mark the command as a backup write and skip locking
      command.addFlags(FlagBitSets.SKIP_LOCKING | FlagBitSets.BACKUP_WRITE);
      command.setValueMatcher(MATCH_ALWAYS);
      command.setTopologyId(topologyId);
      InvocationContextFactory invocationContextFactory = componentRegistry.getInvocationContextFactory().running();
      InvocationContext invocationContext = invocationContextFactory.createRemoteInvocationContextForCommand(command, getOrigin());

      AsyncInterceptorChain interceptorChain = componentRegistry.getInterceptorChain().running();
      return interceptorChain.invokeAsync(invocationContext, command);
   }

   @Override
   public final boolean isReturnValueExpected() {
      return false;
   }

   @Override
   public final boolean canBlock() {
      return true;
   }

   public final long getSequence() {
      return sequence;
   }

   public final CommandInvocationId getCommandInvocationId() {
      return commandInvocationId;
   }

   public final int getTopologyId() {
      return topologyId;
   }

   public final long getFlags() {
      return flags;
   }

   public final int getSegmentId() {
      return segmentId;
   }

   abstract WriteCommand createWriteCommand();

   String toStringFields() {
      return "cacheName=" + cacheName +
            ", segment=" + segmentId +
            ", sequence=" + sequence +
            ", commandInvocationId=" + commandInvocationId +
            ", topologyId=" + topologyId +
            ", flags=" + flags;
   }
}
