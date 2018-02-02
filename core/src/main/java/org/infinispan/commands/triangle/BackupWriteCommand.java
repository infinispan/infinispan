package org.infinispan.commands.triangle;

import static org.infinispan.commands.write.ValueMatcher.MATCH_ALWAYS;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.CompletableFuture;

import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.InvocationContextFactory;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.interceptors.AsyncInterceptorChain;
import org.infinispan.util.ByteString;

/**
 * A write operation sent from the primary owner to the backup owners.
 * <p>
 * This is a base command with the {@link CommandInvocationId}, topology and flags.
 * <p>
 * Since the primary->backup operations are ordered by segment, it contains the segment to be updated and its sequence number.
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
   private int segmentId;

   private InvocationContextFactory invocationContextFactory;
   private AsyncInterceptorChain interceptorChain;

   BackupWriteCommand(ByteString cacheName) {
      super(cacheName);
   }

   @Override
   public final CompletableFuture<Object> invokeAsync() {
      WriteCommand command = createWriteCommand();
      command.setFlagsBitSet(flags);
      command.addFlags(FlagBitSets.SKIP_LOCKING);
      command.setValueMatcher(MATCH_ALWAYS);
      command.setTopologyId(topologyId);
      InvocationContext invocationContext = createContext(command);
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

   final void injectDependencies(InvocationContextFactory factory, AsyncInterceptorChain chain) {
      this.invocationContextFactory = factory;
      this.interceptorChain = chain;
   }

   private InvocationContext createContext(WriteCommand command) {
      return invocationContextFactory.createRemoteInvocationContextForCommand(command, getOrigin());
   }
}
