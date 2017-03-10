package org.infinispan.commands.triangle;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commands.InvocationManager;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.context.InvocationContextFactory;
import org.infinispan.interceptors.AsyncInterceptorChain;
import org.infinispan.metadata.Metadata;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.util.ByteString;
import org.infinispan.util.TriangleFunctionsUtil;

/**
 * A {@link BackupWriteCommand} implementation for {@link PutMapCommand}.
 *
 * @author Pedro Ruivo
 * @since 9.2
 */
public class PutMapBackupWriteCommand extends BackupWriteCommand {

   public static final byte COMMAND_ID = 78;

   private Map<Object, Object> map;
   private Map<Object, CommandInvocationId> lastInvocationIds;
   private Metadata metadata;

   private CacheNotifier cacheNotifier;

   //for testing
   @SuppressWarnings("unused")
   public PutMapBackupWriteCommand() {
      super(null);
   }

   public PutMapBackupWriteCommand(ByteString cacheName) {
      super(cacheName);
   }

   public void init(InvocationContextFactory factory, AsyncInterceptorChain chain,
                    CacheNotifier cacheNotifier, InvocationManager invocationManager) {
      injectDependencies(factory, chain, invocationManager);
      this.cacheNotifier = cacheNotifier;
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public void writeTo(ObjectOutput output) throws IOException {
      writeBase(output);
      MarshallUtil.marshallMap(map, output);
      MarshallUtil.marshallMap(lastInvocationIds, ObjectOutput::writeObject, CommandInvocationId::writeTo, output);
      output.writeObject(metadata);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      readBase(input);
      map = MarshallUtil.unmarshallMap(input, HashMap::new);
      lastInvocationIds = MarshallUtil.unmarshallMap(input, ObjectInput::readObject, CommandInvocationId::readFrom, HashMap::new);
      metadata = (Metadata) input.readObject();
   }

   public void setPutMapCommand(PutMapCommand command, Collection<Object> keys) {
      setCommonAttributesFromCommand(command);
      this.map = TriangleFunctionsUtil.filterEntries(command.getMap(), keys);
      this.metadata = command.getMetadata();
      this.lastInvocationIds = command.getLastInvocationIds();
   }

   @Override
   public String toString() {
      return "PutMapBackupWriteCommand{" + toStringFields() + '}';
   }

   @Override
   WriteCommand createWriteCommand() {
      PutMapCommand cmd = new PutMapCommand(map, cacheNotifier, metadata, getFlags(), getCommandInvocationId(), invocationManager);
      cmd.setForwarded(true);
      cmd.setLastInvocationIds(lastInvocationIds);
      return cmd;
   }

   @Override
   String toStringFields() {
      return super.toStringFields() +
            ", map=" + map +
            ", metadata=" + metadata;
   }
}
