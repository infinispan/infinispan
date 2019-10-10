package org.infinispan.reactive.publisher.impl.commands.batch;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.CompletableFuture;

import org.infinispan.commands.InitializableCommand;
import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.reactive.publisher.impl.PublisherHandler;
import org.infinispan.util.ByteString;
import org.infinispan.util.concurrent.CompletableFutures;

public class CancelPublisherCommand extends BaseRpcCommand implements InitializableCommand {
   public static final byte COMMAND_ID = 49;

   private PublisherHandler publisherHandler;

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
   public CompletableFuture<Object> invokeAsync() throws Throwable {
      publisherHandler.closePublisher(requestId);
      return CompletableFutures.completedNull();
   }

   @Override
   public void init(ComponentRegistry componentRegistry, boolean isRemote) {
      this.publisherHandler = componentRegistry.getPublisherHandler().running();
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
      // This command just removes some references and immediately returns
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
