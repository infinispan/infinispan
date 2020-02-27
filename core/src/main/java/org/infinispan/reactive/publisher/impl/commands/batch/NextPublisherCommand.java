package org.infinispan.reactive.publisher.impl.commands.batch;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.CompletionStage;

import org.infinispan.commands.TopologyAffectedCommand;
import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.reactive.publisher.impl.PublisherHandler;
import org.infinispan.util.ByteString;

public class  NextPublisherCommand extends BaseRpcCommand implements TopologyAffectedCommand {
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
   public CompletionStage<?> invokeAsync(ComponentRegistry componentRegistry) throws Throwable {
      PublisherHandler publisherHandler = componentRegistry.getPublisherHandler().running();
      return publisherHandler.getNext(requestId);
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
   public void writeTo(ObjectOutput output) throws IOException {
      output.writeObject(requestId);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      requestId = input.readObject();
   }
}
