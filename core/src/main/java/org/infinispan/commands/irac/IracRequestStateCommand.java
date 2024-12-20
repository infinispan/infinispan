package org.infinispan.commands.irac;

import java.util.concurrent.CompletionStage;

import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.marshall.protostream.impl.WrappedMessages;
import org.infinispan.protostream.WrappedMessage;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.util.ByteString;

/**
 * Requests the state for a given segments.
 *
 * @author Pedro Ruivo
 * @since 11.0
 */
@ProtoTypeId(ProtoStreamTypeIds.IRAC_REQUEST_STATE_COMMAND)
public class IracRequestStateCommand extends BaseRpcCommand {

   public static final byte COMMAND_ID = 121;

   private IntSet segments;

   public IracRequestStateCommand(ByteString cacheName, IntSet segments) {
      super(cacheName);
      this.segments = segments;
   }

   @ProtoFactory
   IracRequestStateCommand(ByteString cacheName, WrappedMessage segments) {
      this(cacheName, WrappedMessages.<IntSet>unwrap(segments));
   }

   @ProtoField(2)
   WrappedMessage getSegments() {
      return WrappedMessages.orElseNull(segments);
   }

   @Override
   public CompletionStage<?> invokeAsync(ComponentRegistry registry) throws Throwable {
      registry.getIracManager().wired().requestState(getOrigin(), segments);
      return CompletableFutures.completedNull();
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
   public String toString() {
      return "IracRequestStateCommand{" +
            "segments=" + segments +
            ", cacheName=" + cacheName +
            '}';
   }
}
