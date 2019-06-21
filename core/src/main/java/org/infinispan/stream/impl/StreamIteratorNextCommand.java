package org.infinispan.stream.impl;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.CompletableFuture;

import org.infinispan.commands.InitializableCommand;
import org.infinispan.commands.TopologyAffectedCommand;
import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.commons.io.UnsignedNumeric;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.util.ByteString;

/**
 * Stream request command that is sent to remote nodes handle execution of remote intermediate and terminal operations.
 */
public class StreamIteratorNextCommand extends BaseRpcCommand implements InitializableCommand, TopologyAffectedCommand {
   public static final byte COMMAND_ID = 71;

   protected LocalStreamManager lsm;

   protected Object id;
   protected long batchSize;
   protected int topologyId = -1;

   @Override
   public int getTopologyId() {
      return topologyId;
   }

   @Override
   public void setTopologyId(int topologyId) {
      this.topologyId = topologyId;
   }

   public Object getId() {
      return id;
   }

   // Only here for CommandIdUniquenessTest
   private StreamIteratorNextCommand() { super(null); }

   public StreamIteratorNextCommand(ByteString cacheName) {
      super(cacheName);
   }

   public StreamIteratorNextCommand(ByteString cacheName, Object id, long batchSize) {
      super(cacheName);
      this.id = id;
      this.batchSize = batchSize;
   }

   @Override
   public void init(ComponentRegistry componentRegistry, boolean isRemote) {
      this.lsm = componentRegistry.getLocalStreamManager().running();
   }

   @Override
   public CompletableFuture<Object> invokeAsync() throws Throwable {
      return CompletableFuture.completedFuture(lsm.continueIterator(id, batchSize));
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public void writeTo(ObjectOutput output) throws IOException {
      output.writeObject(id);
      UnsignedNumeric.writeUnsignedLong(output, batchSize);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      id = input.readObject();
      batchSize = UnsignedNumeric.readUnsignedLong(input);
   }

   @Override
   public boolean isReturnValueExpected() {
      return true;
   }

   @Override
   public boolean canBlock() {
      return true;
   }
}
