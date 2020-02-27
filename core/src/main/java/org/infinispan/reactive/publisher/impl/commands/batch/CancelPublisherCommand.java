package org.infinispan.reactive.publisher.impl.commands.batch;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.CompletionStage;

import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.reactive.publisher.impl.PublisherHandler;
import org.infinispan.util.ByteString;
import org.infinispan.util.concurrent.CompletableFutures;

public class CancelPublisherCommand extends BaseRpcCommand {
   public static final byte COMMAND_ID = 49;

   private Object requestId;

   // Only here for CommandIdUniquenessTest
   private CancelPublisherCommand() { super(null); }

   public CancelPublisherCommand(ByteString cacheName) {
      super(cacheName);
   }

   public CancelPublisherCommand(ByteString cacheName, Object requestId) {
      super(cacheName);
      this.requestId = requestId;
   }

   @Override
   public CompletionStage<?> invokeAsync(ComponentRegistry componentRegistry) throws Throwable {
      PublisherHandler publisherHandler = componentRegistry.getPublisherHandler().running();
      publisherHandler.closePublisher(requestId);
      return CompletableFutures.completedNull();
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
