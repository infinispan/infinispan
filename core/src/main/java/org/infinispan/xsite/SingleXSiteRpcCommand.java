package org.infinispan.xsite;

import java.util.concurrent.CompletionStage;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.marshall.protostream.impl.MarshallableObject;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.telemetry.InfinispanSpanAttributes;
import org.infinispan.util.ByteString;
import org.infinispan.xsite.commands.remote.XSiteCacheRequest;

/**
 * RPC command to replicate cache operations (such as put, remove, replace, etc.) to the backup site.
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
@ProtoTypeId(ProtoStreamTypeIds.XSITE_SINGLE_RPC_COMMAND)
public class SingleXSiteRpcCommand extends XSiteCacheRequest<Object> {

   private final VisitableCommand command;
   private InfinispanSpanAttributes spanAttributes;

   public SingleXSiteRpcCommand(ByteString cacheName, VisitableCommand command) {
      super(cacheName);
      this.command = command;
   }

   @ProtoFactory
   SingleXSiteRpcCommand(ByteString cacheName, MarshallableObject<VisitableCommand> command) {
      this(cacheName, MarshallableObject.unwrap(command));
   }

   @ProtoField(2)
   MarshallableObject<VisitableCommand> getCommand() {
      return MarshallableObject.create(command);
   }

   @Override
   protected CompletionStage<Object> invokeInLocalCache(String origin, ComponentRegistry registry) {
      return registry.getBackupReceiver().running().handleRemoteCommand(command);
   }

   @Override
   public InfinispanSpanAttributes getSpanAttributes() {
      return spanAttributes;
   }

   @Override
   public String getOperationName() {
      // TODO use the class name or implement this method in all commands?
      return command.getClass().getSimpleName();
   }

   @Override
   public void setSpanAttributes(InfinispanSpanAttributes attributes) {
      spanAttributes = attributes;
   }

   @Override
   public String toString() {
      return "SingleXSiteRpcCommand{" +
            "command=" + command +
            '}';
   }
}
