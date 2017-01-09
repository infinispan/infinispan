package org.infinispan.commands.write;

import java.io.DataInput;
import java.io.DataOutput;
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
public class BackupPutMapRcpCommand extends BaseRpcCommand implements TopologyAffectedCommand {

   public static final byte COMMAND_ID = 66;
   private CommandInvocationId commandInvocationId;
   private Map<Object, Object> map;
   private Metadata metadata;
   private long flags;
   private int topologyId;
   private Map<Integer, Long> segmentsAndSequences;
   private InvocationContextFactory invocationContextFactory;
   private AsyncInterceptorChain interceptorChain;
   private CacheNotifier cacheNotifier;

   public BackupPutMapRcpCommand() {
      super(null);
   }

   public BackupPutMapRcpCommand(ByteString cacheName, PutMapCommand command) {
      super(cacheName);
      this.metadata = command.getMetadata();
      this.flags = command.getFlagsBitSet();
      this.topologyId = command.getTopologyId();
      this.commandInvocationId = command.getCommandInvocationId();
   }

   public BackupPutMapRcpCommand(ByteString cacheName) {
      super(cacheName);
   }

   public void setMap(Map<Object, Object> map) {
      this.map = map;
   }

   public Map<Integer, Long> getSegmentsAndSequences() {
      return segmentsAndSequences;
   }

   public void setSegmentsAndSequences(Map<Integer, Long> segmentsAndSequences) {
      this.segmentsAndSequences = segmentsAndSequences;
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
   public void writeTo(ObjectOutput output) throws IOException {
      CommandInvocationId.writeTo(output, commandInvocationId);
      MarshallUtil.marshallMap(map, output);
      output.writeObject(metadata);
      output.writeLong(FlagBitSets.copyWithoutRemotableFlags(flags));
      MarshallUtil.marshallMap(segmentsAndSequences, DataOutput::writeInt, DataOutput::writeLong, output);

   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      commandInvocationId = CommandInvocationId.readFrom(input);
      map = MarshallUtil.unmarshallMap(input, HashMap::new);
      metadata = (Metadata) input.readObject();
      flags = input.readLong();
      segmentsAndSequences = MarshallUtil.unmarshallMap(input, DataInput::readInt, DataInput::readLong, HashMap::new);
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
}
