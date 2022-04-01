package org.infinispan.commands.remote;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.CompletionStage;

import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.scattered.BiasManager;
import org.infinispan.util.ByteString;
import org.infinispan.commons.util.concurrent.CompletableFutures;

public class RenewBiasCommand extends BaseRpcCommand {
   public static final byte COMMAND_ID = 75;

   Object[] keys;

   public RenewBiasCommand() {
      super(null);
   }

   public RenewBiasCommand(ByteString cacheName) {
      super(cacheName);
   }

   public RenewBiasCommand(ByteString cacheName, Object[] keys) {
      super(cacheName);
      this.keys = keys;
   }

   @Override
   public CompletionStage<?> invokeAsync(ComponentRegistry componentRegistry) throws Throwable {
      BiasManager biasManager = componentRegistry.getBiasManager().running();
      for (Object key : keys) {
         biasManager.renewRemoteBias(key, getOrigin());
      }
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
      MarshallUtil.marshallArray(keys, output);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      keys = MarshallUtil.unmarshallArray(input, Object[]::new);
   }
}
