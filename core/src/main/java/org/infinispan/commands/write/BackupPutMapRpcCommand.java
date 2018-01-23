package org.infinispan.commands.write;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commands.TopologyAffectedCommand;
import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.commons.util.EnumUtil;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.InvocationContextFactory;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.interceptors.AsyncInterceptorChain;
import org.infinispan.metadata.Metadata;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.util.ByteString;

/**
 * A command sent from the primary owner to the backup owners for a {@link PutMapCommand}.
 * <p>
 * This command is only visited by the backups owner and in a remote context. The command order is set by {@code
 * segmentsAndSequences} map.
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
public class BackupPutMapRpcCommand extends BaseRpcCommand implements TopologyAffectedCommand {

   public static final byte COMMAND_ID = 66;
   private CommandInvocationId commandInvocationId;
   private Map<Object, Object> map;
   private Metadata metadata;
   private long flags;
   private int topologyId;
   private long sequence;
   private InvocationContextFactory invocationContextFactory;
   private AsyncInterceptorChain interceptorChain;
   private CacheNotifier cacheNotifier;

   public BackupPutMapRpcCommand() {
      super(null);
   }

   public BackupPutMapRpcCommand(ByteString cacheName, PutMapCommand command) {
      super(cacheName);
      this.metadata = command.getMetadata();
      this.flags = command.getFlagsBitSet();
      this.topologyId = command.getTopologyId();
      this.commandInvocationId = command.getCommandInvocationId();
   }

   public BackupPutMapRpcCommand(ByteString cacheName) {
      super(cacheName);
   }

   public Map<Object, Object> getMap() {
      return map;
   }

   public void setMap(Map<Object, Object> map) {
      this.map = map;
   }

   public CommandInvocationId getCommandInvocationId() {
      return commandInvocationId;
   }

   public void init(InvocationContextFactory invocationContextFactory, AsyncInterceptorChain interceptorChain,
         CacheNotifier cacheNotifier) {
      this.invocationContextFactory = invocationContextFactory;
      this.interceptorChain = interceptorChain;
      this.cacheNotifier = cacheNotifier;
   }

   @Override
   public int getTopologyId() {
      return topologyId;
   }

   @Override
   public void setTopologyId(int topologyId) {
      this.topologyId = topologyId;
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public boolean isReturnValueExpected() {
      return false;
   }

   @Override
   public boolean canBlock() {
      return true;
   }

   @Override
   public void writeTo(ObjectOutput output) throws IOException {
      CommandInvocationId.writeTo(output, commandInvocationId);
      MarshallUtil.marshallMap(map, output);
      output.writeObject(metadata);
      output.writeLong(FlagBitSets.copyWithoutRemotableFlags(flags));
      output.writeLong(sequence);

   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      commandInvocationId = CommandInvocationId.readFrom(input);
      map = MarshallUtil.unmarshallMap(input, HashMap::new);
      metadata = (Metadata) input.readObject();
      flags = input.readLong();
      sequence = input.readLong();
   }

   @Override
   public CompletableFuture<Object> invokeAsync() throws Throwable {
      PutMapCommand command = new PutMapCommand(map, cacheNotifier, metadata, flags, commandInvocationId);
      command.addFlags(FlagBitSets.SKIP_LOCKING);
      command.setValueMatcher(ValueMatcher.MATCH_ALWAYS);
      command.setTopologyId(topologyId);
      command.setForwarded(true);
      InvocationContext invocationContext = invocationContextFactory
            .createRemoteInvocationContextForCommand(command, getOrigin());
      return interceptorChain.invokeAsync(invocationContext, command);
   }

   @Override
   public String toString() {
      return "BackupPutMapRpcCommand{" +
            "commandInvocationId=" + commandInvocationId +
            ", map=" + map +
            ", metadata=" + metadata +
            ", flags=" + EnumUtil.prettyPrintBitSet(flags, Flag.class) +
            ", topologyId=" + topologyId +
            ", sequence=" + sequence +
            '}';
   }

   public long getSequence() {
      return sequence;
   }

   public void setSequence(long sequence) {
      this.sequence = sequence;
   }

   public long getFlagBitSet() {
      return flags;
   }
}
