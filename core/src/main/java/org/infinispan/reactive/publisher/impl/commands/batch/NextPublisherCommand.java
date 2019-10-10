package org.infinispan.reactive.publisher.impl.commands.batch;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.CompletableFuture;

import org.infinispan.commands.InitializableCommand;
import org.infinispan.commands.TopologyAffectedCommand;
import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.reactive.publisher.impl.PublisherHandler;
import org.infinispan.util.ByteString;

public class NextPublisherCommand extends BaseRpcCommand implements InitializableCommand, TopologyAffectedCommand {
   public static final byte COMMAND_ID = 25;

   private PublisherHandler publisherHandler;

   private Object requestId;
   private int topologyId = -1;

   // Only here for CommandIdUniquenessTest
   private NextPublisherCommand() { super(null); }

   public NextPublisherCommand(ByteString cacheName) {
      super(cacheName);
   }

   public NextPublisherCommand(ByteString cacheName, Object requestId) {
      super(cacheName);
      this.requestId = requestId;
   }

   @Override
   public CompletableFuture<Object> invokeAsync() throws Throwable {
      return (CompletableFuture) publisherHandler.getNext(requestId);
   }

   @Override
   public void init(ComponentRegistry componentRegistry, boolean isRemote) {
      this.publisherHandler = componentRegistry.getPublisherHandler().running();
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
      return true;
   }

   @Override
   public boolean canBlock() {
      // This command is guaranteed to only use CPU now - stores are done in a blocking thread pool
      return false;
   }

   @Override
   public void writeTo(ObjectOutput output) throws IOException {
      output.writeObject(requestId);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      requestId = input.readObject();
   }
}
