package org.infinispan.commands.triangle;

import static org.infinispan.commands.write.ValueMatcher.MATCH_ALWAYS;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.CompletionStage;

import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.InvocationContextFactory;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.interceptors.AsyncInterceptorChain;
import org.infinispan.util.ByteString;
import org.infinispan.util.concurrent.CompletableFutures;

/**
 * A write operation sent from the primary owner to the backup owners.
 * <p>
 * This is a base command with the {@link CommandInvocationId}, topology and flags.
 * <p>
 * Since the primary &rarr; backup operations are ordered by segment, it contains the segment to be updated and its sequence number.
 *
 * @author Pedro Ruivo
 * @since 9.2
 */
public abstract class BackupWriteCommand extends BaseRpcCommand {

   //common attributes of all write commands
   private CommandInvocationId commandInvocationId;
   private int topologyId;
   private long flags;

   //backup commands are ordered by segment. this is the sequence number of the segment.
   private long sequence;
   protected int segmentId;

   BackupWriteCommand(ByteString cacheName) {
      super(cacheName);
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

   public final void setSequence(long sequence) {
      this.sequence = sequence;
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

   public final void setSegmentId(int segmentId) {
      this.segmentId = segmentId;
   }

   final void writeBase(ObjectOutput output) throws IOException {
      CommandInvocationId.writeTo(output, commandInvocationId);
      output.writeInt(topologyId);
      output.writeLong(flags);
      output.writeLong(sequence);
      output.writeInt(segmentId);
   }

   final void readBase(ObjectInput input) throws IOException, ClassNotFoundException {
      commandInvocationId = CommandInvocationId.readFrom(input);
      topologyId = input.readInt();
      flags = input.readLong();
      sequence = input.readLong();
      segmentId = input.readInt();
   }

   void setCommonAttributesFromCommand(WriteCommand command) {
      this.commandInvocationId = command.getCommandInvocationId();
      this.topologyId = command.getTopologyId();
      this.flags = command.getFlagsBitSet();
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
