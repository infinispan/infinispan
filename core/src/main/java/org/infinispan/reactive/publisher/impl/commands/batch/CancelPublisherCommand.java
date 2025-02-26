package org.infinispan.reactive.publisher.impl.commands.batch;

import java.util.concurrent.CompletionStage;

import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.reactive.publisher.impl.PublisherHandler;
import org.infinispan.util.ByteString;
import org.infinispan.commons.util.concurrent.CompletableFutures;

@ProtoTypeId(ProtoStreamTypeIds.CANCEL_PUBLISHER_COMMAND)
public class CancelPublisherCommand extends BaseRpcCommand {

   @ProtoField(2)
   final String requestId;

   @ProtoFactory
   public CancelPublisherCommand(ByteString cacheName, String requestId) {
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
   public boolean isReturnValueExpected() {
      return true;
   }
}
