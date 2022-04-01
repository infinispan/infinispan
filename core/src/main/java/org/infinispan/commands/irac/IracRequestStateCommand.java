package org.infinispan.commands.irac;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.CompletionStage;

import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.IntSetsExternalization;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.util.ByteString;
import org.infinispan.commons.util.concurrent.CompletableFutures;

/**
 * Requests the state for a given segments.
 *
 * @author Pedro Ruivo
 * @since 11.0
 */
public class IracRequestStateCommand extends BaseRpcCommand {

   public static final byte COMMAND_ID = 121;

   private IntSet segments;

   @SuppressWarnings("unused")
   public IracRequestStateCommand() {
      super(null);
   }

   public IracRequestStateCommand(ByteString cacheName) {
      super(cacheName);
   }

   public IracRequestStateCommand(ByteString cacheName, IntSet segments) {
      super(cacheName);
      this.segments = segments;
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
   public void writeTo(ObjectOutput output) throws IOException {
      IntSetsExternalization.writeTo(output, segments);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      this.segments = IntSetsExternalization.readFrom(input);
   }

   @Override
   public String toString() {
      return "IracRequestStateCommand{" +
            "segments=" + segments +
            ", cacheName=" + cacheName +
            '}';
   }
}
