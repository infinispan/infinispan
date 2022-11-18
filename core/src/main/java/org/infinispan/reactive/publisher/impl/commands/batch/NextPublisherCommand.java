package org.infinispan.reactive.publisher.impl.commands.batch;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.CompletionStage;

import org.infinispan.commands.remote.BaseTopologyRpcCommand;
import org.infinispan.commons.util.EnumUtil;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.reactive.publisher.impl.PublisherHandler;
import org.infinispan.util.ByteString;

public class NextPublisherCommand extends BaseTopologyRpcCommand {
   public static final byte COMMAND_ID = 25;

   private String requestId;

   // Only here for CommandIdUniquenessTest
   private NextPublisherCommand() {
      this(null);
   }

   public NextPublisherCommand(ByteString cacheName) {
      super(cacheName, EnumUtil.EMPTY_BIT_SET);
   }

   public NextPublisherCommand(ByteString cacheName, String requestId) {
      this(cacheName);
      this.requestId = requestId;
   }

   @Override
   public CompletionStage<?> invokeAsync(ComponentRegistry componentRegistry) throws Throwable {
      PublisherHandler publisherHandler = componentRegistry.getPublisherHandler().running();
      return publisherHandler.getNext(requestId);
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
      output.writeUTF(requestId);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      requestId = input.readUTF();
   }
}
